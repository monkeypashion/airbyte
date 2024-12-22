import sys
import os
import time
import requests
from typing import Any, Dict, Iterable, List, Mapping
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
    def __init__(self):
        super().__init__()

    def filter_qc_properties(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Remove fields ending with '_qc'."""
        return {k: v for k, v in record.items() if not k.endswith('_qc')}

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
                    "tenant_id"
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
            authenticator = get_authenticator(config)
            if authenticator.check_connection():
                logger.debug("Successfully connected to the destination")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                logger.error("Failed to connect to the destination")
                return AirbyteConnectionStatus(status=Status.FAILED, message="Unable to authenticate with the destination")
        except Exception as e:
            logger.error(
                f"Exception while connecting to the destination: {str(e)}")
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        logger.debug("Starting the write process")
        authenticator = get_authenticator(config)
        access_token = authenticator.get_token()

        api_url = f"https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/timeseries/{config['asset_id']}/{config['aspect_id']}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        # Create a copy of headers for logging with redacted token
        log_headers = headers.copy()
        log_headers["Authorization"] = "Bearer [REDACTED]"

        payload = []
        for message in input_messages:
            try:
                if message.type == Type.RECORD:
                    filtered_record = self.filter_qc_properties(
                        message.record.data)
                    payload.append(filtered_record)

                    if len(payload) >= 100:
                        self._send_payload(
                            api_url, headers, payload, log_headers)
                        payload.clear()

                elif message.type == Type.STATE:
                    logger.debug(f"Processing state message: {message.state}")
                    yield message

            except ValueError as e:
                logger.error(f"Error processing message: {e}")
                raise  # Allow Airbyte to capture and report the failure

        if payload:
            logger.debug(f"Sending final batch of {len(payload)} records")
            self._send_payload(api_url, headers, payload, log_headers)

        logger.debug("Write process completed successfully")

    def _send_payload(self, url: str, headers: dict, payload: List[dict], log_headers: dict, max_retries: int = 5) -> None:
        retry_delay = 1  # Initial delay for exponential backoff
        session = requests.Session()  # Use a session for connection pooling

        for attempt in range(max_retries):
            try:
                logger.debug(f"Sending PUT request to URL: {url}")
                response = session.put(
                    url, headers=headers, json=payload, timeout=30)

                if response.status_code in {200, 204}:
                    logger.debug(
                        f"Successfully sent {len(payload)} records to {url}")
                    return  # Request succeeded

                elif response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        retry_delay = int(retry_after)
                        logger.warning(
                            f"Rate limited. Retrying after {retry_delay} seconds (Retry-After header).")
                    else:
                        logger.warning(
                            f"Rate limited. Retry-After header missing, falling back to {retry_delay} seconds.")

                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff for subsequent retries

                else:
                    logger.error(f"Request failed for URL: {url}")
                    logger.error(f"Request Headers: {log_headers}")
                    logger.error(f"Request Payload: {payload}")
                    logger.error(
                        f"Response Status Code: {response.status_code}")
                    logger.error(f"Response Content: {response.text}")
                    response.raise_for_status()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for URL: {url}")
                logger.error(f"Request Headers: {log_headers}")
                logger.error(f"Request Payload: {payload}")

                if hasattr(e, 'response') and e.response is not None:
                    logger.error(
                        f"Response Status Code: {e.response.status_code}")
                    logger.error(f"Response Content: {e.response.text}")

                if attempt < max_retries - 1 and hasattr(e, 'response') and e.response and e.response.status_code in {429, 500, 502, 503, 504}:
                    logger.warning(
                        f"Retrying after failure ({attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(
                        f"Failed to send payload after {attempt + 1} attempts: {str(e)}")
                    raise ValueError(
                        f"Failed to send payload after {attempt + 1} attempts: {str(e)}") from e

        raise ValueError(
            f"Failed to send payload after {max_retries} attempts due to persistent issues.")
