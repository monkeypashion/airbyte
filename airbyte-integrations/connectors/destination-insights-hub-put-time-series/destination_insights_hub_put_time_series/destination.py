import time
import requests
from typing import Any, Dict, Iterable, List, Mapping, Optional
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteConnectionStatus,
    Status,
    ConnectorSpecification,
    ConfiguredAirbyteCatalog,
    Type,
    AirbyteTraceMessage,
)


class OAuthAuthenticator:
    """Manages OAuth2 authentication for Insights Hub API."""

    def __init__(
        self,
        token_refresh_endpoint: str,
        client_id: str,
        client_secret: str,
        access_token_name: str = "access_token",
        expires_in_name: str = "expires_in",
    ):
        self.token_refresh_endpoint = token_refresh_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token_name = access_token_name
        self.expires_in_name = expires_in_name
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None

    def get_access_token(self) -> str:
        if self.access_token and self.token_expires_at and self.token_expires_at > time.time():
            return self.access_token

        try:
            response = requests.post(
                self.token_refresh_endpoint,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=30,
            )
            response.raise_for_status()
            token_response = response.json()
            self.access_token = token_response[self.access_token_name]
            expires_in = token_response.get(self.expires_in_name, 3600)
            self.token_expires_at = time.time() + expires_in - 60
            return self.access_token
        except requests.RequestException as e:
            raise ValueError(f"Token retrieval failed: {str(e)}") from e


def get_authenticator(config: dict) -> OAuthAuthenticator:
    token_refresh_endpoint = f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token"
    return OAuthAuthenticator(
        token_refresh_endpoint=token_refresh_endpoint,
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )


class DestinationInsightsHubPutTimeSeries(Destination):
    def __init__(self):
        super().__init__()
        self.logger = self._setup_logger()

    def _setup_logger(self):
        import logging
        logger = logging.getLogger("DestinationApiPut")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
        return logger

    def filter_qc_properties(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Remove fields ending with '_qc'."""
        return {k: v for k, v in record.items() if not k.endswith('_qc')}

    def spec(self, logger=None) -> ConnectorSpecification:
        """
        Define the specification for the destination connector.

        :param logger: Optional logger (added for compatibility)
        :return: Connector configuration specification
        """

        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/destinations/ih-api-timeseries",
            supported_destination_sync_modes=["overwrite", "append"],
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Insights Hub Put Timeseries",
                "type": "object",
                "required": [
                    "asset_id",
                    "aspect_id",
                    "client_id",
                    "client_secret",
                    "tenant_id"
                ],
                "properties": {
                    "asset_id": {
                        "type": "string",
                        "title": "Asset ID",
                        "description": "Insights Hub Asset ID",
                        "order": 0
                    },
                    "aspect_id": {
                        "type": "string",
                        "title": "Aspect ID",
                        "description": "Insights Hub Aspect ID",
                        "order": 1
                    },
                    "client_id": {
                        "type": "string",
                        "title": "Client ID",
                        "description": "OAuth Client ID (Technical User Name)",
                        "airbyte_secret": True,
                        "order": 2
                    },
                    "client_secret": {
                        "type": "string",
                        "title": "Client Secret",
                        "description": "OAuth Client Secret (Technical User Password)",
                        "airbyte_secret": True,
                        "order": 3
                    },
                    "tenant_id": {
                        "type": "string",
                        "title": "Tenant ID",
                        "description": "Tenant ID",
                        "order": 4
                    }
                }
            }
        )

    def check(self, logger, config: dict) -> AirbyteConnectionStatus:
        """
        Check the connection by verifying token retrieval.
        
        :param logger: Logging object
        :param config: Configuration dictionary
        :return: Connection status
        """
        try:
            # Attempt to retrieve access token
            authenticator = get_authenticator(config)
            access_token = authenticator.get_access_token()

            # Log the retrieved token
            logger.info(f"Retrieved access token: {access_token}")

            # Validate token retrieval
            logger.info("Successfully retrieved access token")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            # Log and return failure status
            error_msg = f"Connection check failed: {str(e)}"
            logger.error(error_msg)
            return AirbyteConnectionStatus(status=Status.FAILED, message=error_msg)

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        """
        Processes and sends records to the destination.
        """
        self.logger.info("Starting the write process...")

        # Initialize authenticator
        authenticator = get_authenticator(config)
        access_token = authenticator.get_access_token()

        # Prepare the API endpoint and headers
        api_url = f"https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/timeseries/{config['asset_id']}/{config['aspect_id']}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        payload = []
        for message in input_messages:
            if message.type == Type.RECORD:
                filtered_record = self.filter_qc_properties(message.record.data)
                payload.append(filtered_record)
                self.logger.debug(f"Queued record: {filtered_record}")

                # Send batch if payload size exceeds threshold
                if len(payload) >= 100:
                    self._send_payload(api_url, headers, payload)
                    payload.clear()
            elif message.type == Type.STATE:
                self.logger.info(f"Processing state message: {message.state}")
                yield message

        # Send remaining payload
        if payload:
            self.logger.info(f"Sending final batch of {len(payload)} records.")
            self._send_payload(api_url, headers, payload)

        self.logger.info("Write process completed successfully.")


    def _send_payload(self, url: str, headers: dict, payload: List[dict], max_retries: int = 5) -> None:
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                response = requests.put(url, headers=headers, json=payload, timeout=30)
                response.raise_for_status()
                self.logger.info(f"Successfully sent {len(payload)} records to {url}")
                return
            except requests.RequestException as e:
                if attempt < max_retries - 1 and response.status_code in {429, 500, 502, 503, 504}:
                    self.logger.warning(f"Retrying after failure: {str(e)}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    raise ValueError(f"Failed to send payload after {attempt + 1} attempts: {str(e)}") from e
