import sys
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
)
from .utils.oauth_authenticator import OAuthAuthenticator
import logging

# Use Airbyte's logger directly without modification
logger = logging.getLogger("airbyte")


def get_authenticator(config: Mapping[str, Any]) -> OAuthAuthenticator:
    return OAuthAuthenticator(
        token_refresh_endpoint=f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token",
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )


class DestinationInsightsHubPutTimeSeries(Destination):
    def __init__(self, config: Optional[Mapping[str, Any]] = None, debug_mode: bool = False):
        super().__init__()
        self.config = config
        self.authenticator = None
        self.debug_mode = debug_mode
        # Default batch size; can be made configurable if needed.
        self.batch_size = 100

        if config:
            self.authenticator = get_authenticator(config)

    def log_payload(self, payload: List[Dict[str, Any]]):
        """Log payload details based on debug mode."""
        if self.debug_mode:
            logger.debug(
                f"Payload: {payload[:3]}... (total {len(payload)} records)"
            )
        else:
            key_identifiers = [record.get("id", "unknown")
                               for record in payload[:3]]
            logger.debug(
                f"Processed {len(payload)} records. Sample IDs: {key_identifiers}..."
            )

    def filter_qc_properties(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Remove fields ending with '_qc'."""
        return {k: v for k, v in record.items() if not k.endswith('_qc')}

    def validate_config(self, config: Mapping[str, Any]):
        """Validate required configuration fields."""
        required_fields = ["asset_id", "aspect_id",
                           "client_id", "client_secret", "tenant_id"]
        missing_fields = [
            field for field in required_fields if field not in config]
        if missing_fields:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing_fields)}"
            )

    def spec(self, logger=None) -> ConnectorSpecification:
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
                    "tenant_id",
                ],
                "properties": {
                    "asset_id": {"type": "string", "title": "Asset ID", "description": "Insights Hub Asset ID", "order": 0},
                    "aspect_id": {"type": "string", "title": "Aspect ID", "description": "Insights Hub Aspect ID", "order": 1},
                    "client_id": {"type": "string", "title": "Client ID", "description": "OAuth Client ID", "airbyte_secret": True, "order": 2},
                    "client_secret": {"type": "string", "title": "Client Secret", "description": "OAuth Client Secret", "airbyte_secret": True, "order": 3},
                    "tenant_id": {"type": "string", "title": "Tenant ID", "description": "Tenant ID", "order": 4},
                },
            }
        )

    def check(self, logger, config: dict) -> AirbyteConnectionStatus:
        try:
            self.validate_config(config)

            if not self.authenticator:
                self.authenticator = get_authenticator(config)

            if self.authenticator.check_connection():
                logger.info("Successfully connected to the destination.")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                logger.error("Failed to connect to the destination.")
                return AirbyteConnectionStatus(status=Status.FAILED, message="Unable to authenticate with the destination.")
        except Exception as e:
            logger.error(
                f"Exception while connecting to the destination: {str(e)}"
            )
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))

    def _send_payload(self, url: str, headers: dict, payload: List[dict], log_headers: dict, max_retries: int = 5) -> bool:
        retry_delay = 1  # Initial delay for exponential backoff
        session = requests.Session()

        for attempt in range(max_retries):
            try:
                # Refresh token before each attempt
                headers["Authorization"] = f"Bearer {self.authenticator.get_token()}"
                log_headers["Authorization"] = "Bearer [REDACTED]"

                logger.debug(f"Sending PUT request to URL: {url}")
                response = session.put(
                    url, headers=headers, json=payload, timeout=30)

                if response.status_code in {200, 204}:
                    logger.debug(
                        f"Successfully sent {len(payload)} records to {url}"
                    )
                    return True

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    retry_delay = int(
                        retry_after) if retry_after else retry_delay
                    logger.warning(
                        f"Rate limited. Retrying after {retry_delay} seconds."
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2

                else:
                    logger.error(
                        f"Unexpected response. Status: {response.status_code}, Content: {response.text}"
                    )
                    logger.error(f"Request Headers: {log_headers}")
                    logger.error(f"Request Payload: {payload}")
                    response.raise_for_status()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for URL: {url}")
                logger.error(f"Request Headers: {log_headers}")
                logger.error(f"Request Payload: {payload}")

                if hasattr(e, 'response') and e.response is not None:
                    logger.error(
                        f"Response Status Code: {e.response.status_code}"
                    )
                    logger.error(f"Response Content: {e.response.text}")

                if attempt < max_retries - 1 and hasattr(e, 'response') and e.response and e.response.status_code in {429, 500, 502, 503, 504}:
                    logger.warning(
                        f"Retrying after failure ({attempt + 1}/{max_retries}): {str(e)}"
                    )
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(
                        f"Failed to send payload after {attempt + 1} attempts: {str(e)}"
                    )
                    return False

        logger.error(
            f"Failed to send payload after {max_retries} attempts due to persistent issues."
        )
        return False

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        logger.info("Starting the write process.")

        self.validate_config(config)

        if not self.authenticator:
            self.authenticator = get_authenticator(config)

        api_url = f"https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/timeseries/{config['asset_id']}/{config['aspect_id']}"

        headers = {
            "Authorization": f"Bearer {self.authenticator.get_token()}",
            "Content-Type": "application/json",
        }

        log_headers = headers.copy()
        log_headers["Authorization"] = "Bearer [REDACTED]"

        payload = []
        last_successful_state = None
        resumed = False

        # Metrics counters
        total_records_processed = 0
        batches_sent = 0

        for message in input_messages:
            try:
                if message.type == Type.RECORD:
                    filtered_record = self.filter_qc_properties(
                        message.record.data)
                    payload.append(filtered_record)

                    if len(payload) >= self.batch_size:
                        success = self._send_payload(
                            api_url, headers, payload, log_headers)
                        if success:
                            latest_timestamp = max(record.get(
                                "_time", "unknown") for record in payload)
                            total_records_processed += len(payload)
                            batches_sent += 1
                            logger.info(
                                f"Batch {batches_sent} processed. Up to timestamp: {latest_timestamp}. "
                                f"Total records processed: {total_records_processed}."
                            )
                            if last_successful_state:
                                yield last_successful_state
                                last_successful_state = None
                            payload.clear()
                        else:
                            if last_successful_state:
                                yield last_successful_state
                            raise ValueError("Failed batch retries.")

                elif message.type == Type.STATE:
                    if not resumed and last_successful_state:
                        logger.info(
                            "Resuming sync after failure. "
                            f"Starting from state: {last_successful_state.state}."
                        )
                        resumed = True

                    last_successful_state = message
                    logger.debug(f"Stored state message: {message.state}")

            except Exception as e:
                logger.error(f"Error during write process: {str(e)}")
                raise

        if payload:
            success = self._send_payload(
                api_url, headers, payload, log_headers)
            if success:
                latest_timestamp = max(record.get(
                    "_time", "unknown") for record in payload)
                total_records_processed += len(payload)
                batches_sent += 1
                logger.info(
                    f"Final batch processed. Up to timestamp: {latest_timestamp}. "
                    f"Total records processed: {total_records_processed}."
                )
                if last_successful_state:
                    yield last_successful_state
            else:
                raise ValueError("Failed to send the final payload.")

        logger.info(
            f"Write completed successfully. Total records processed: {total_records_processed}, Batches sent: {batches_sent}."
        )
