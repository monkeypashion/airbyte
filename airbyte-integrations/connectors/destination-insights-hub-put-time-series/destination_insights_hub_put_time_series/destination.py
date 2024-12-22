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


def get_authenticator(config: Mapping[str, Any]) -> OAuthAuthenticator:
    return OAuthAuthenticator(
        token_refresh_endpoint=f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token",
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )


class DestinationInsightsHubPutTimeSeries(Destination):
    def __init__(self):
        super().__init__()
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("DestinationApiPut")
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(handler)
        return logger

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
                logger.info("Successfully connected to the destination")
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
        self.logger.info("Starting the write process...")
        authenticator = get_authenticator(config)
        access_token = authenticator.get_token()

        api_url = f"https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/timeseries/{config['asset_id']}/{config['aspect_id']}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        payload = []
        for message in input_messages:
            if message.type == Type.RECORD:
                filtered_record = self.filter_qc_properties(
                    message.record.data)
                payload.append(filtered_record)

                if len(payload) >= 100:
                    self._send_payload(api_url, headers, payload)
                    payload.clear()
            elif message.type == Type.STATE:
                self.logger.info(f"Processing state message: {message.state}")
                yield message

        if payload:
            self.logger.info(f"Sending final batch of {len(payload)} records.")
            self._send_payload(api_url, headers, payload)

        self.logger.info("Write process completed successfully.")

    def _send_payload(self, url: str, headers: dict, payload: List[dict], max_retries: int = 5) -> None:
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Sending PUT request to URL: {url}")
                response = requests.put(
                    url, headers=headers, json=payload, timeout=30)
                response.raise_for_status()
                self.logger.info(
                    f"Successfully sent {len(payload)} records to {url}")
                return
            except requests.RequestException as e:
                # Log full request and response details on failure
                if hasattr(e, 'response') and e.response is not None:
                    self.logger.error(f"Request failed for URL: {url}")
                    self.logger.error(f"Request Headers: {headers}")
                    self.logger.error(f"Request Payload: {payload}")
                    self.logger.error(
                        f"Response Status Code: {e.response.status_code}")
                    self.logger.error(f"Response Content: {e.response.text}")

                if attempt < max_retries - 1 and hasattr(e.response, 'status_code') and e.response.status_code in {429, 500, 502, 503, 504}:
                    self.logger.warning(
                        f"Retrying after failure ({attempt + 1}/{max_retries}): {str(e)}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    self.logger.error(
                        f"Failed to send payload after {attempt + 1} attempts: {str(e)}")
                    raise ValueError(
                        f"Failed to send payload after {attempt + 1} attempts: {str(e)}") from e
