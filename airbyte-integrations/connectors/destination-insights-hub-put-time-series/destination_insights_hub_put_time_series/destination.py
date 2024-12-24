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
    AirbyteLogMessage
)
from .utils.oauth_authenticator import OAuthAuthenticator


def log_message(message: str, level: str = "INFO"):
    log_message = AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=level, message=message)
    )
    print(log_message.json())  # Send to stdout for Airbyte to parse


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
        self.batch_size = 100
        self.session = requests.Session()  # Initialize session unconditionally

        if config:
            self.authenticator = get_authenticator(config)

    def log_payload(self, payload: List[Dict[str, Any]]):
        if self.debug_mode:
            log_message(
                f"Payload: {payload[:3]}... (total {len(payload)} records)",
                level="DEBUG"
            )
        else:
            key_identifiers = [record.get("id", "unknown")
                               for record in payload[:3]]
            log_message(
                f"Processed {len(payload)} records. Sample IDs: {key_identifiers}...",
                level="DEBUG"
            )

    def filter_qc_properties(self, record: Dict[str, Any]) -> Dict[str, Any]:
        return {k: v for k, v in record.items() if not k.endswith('_qc')}

    def validate_config(self, config: Mapping[str, Any]):
        required_fields = ["asset_id", "aspect_id",
                           "client_id", "client_secret", "tenant_id"]
        missing_fields = [
            field for field in required_fields if field not in config]
        if missing_fields:
            raise ValueError(
                f"Missing required configuration fields: {', '.join(missing_fields)}")

    def spec(self, logger=None) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/destinations/ih-api-timeseries",
            supported_destination_sync_modes=["overwrite", "append"],
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Insights Hub Put Timeseries",
                "type": "object",
                "required": ["asset_id", "aspect_id", "client_id", "client_secret", "tenant_id"],
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
                log_message(
                    "Successfully connected to the destination.", level="INFO")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                log_message(
                    "Unable to authenticate with the destination.", level="ERROR")
                return AirbyteConnectionStatus(status=Status.FAILED, message="Unable to authenticate with the destination.")
        except Exception as e:
            log_message(f"Connection check failed: {str(e)}", level="ERROR")
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))

    def _send_payload(self, url: str, payload: List[dict], max_retries: int = 5) -> bool:
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                # Get a fresh token for each attempt - this will refresh if needed
                token = self.authenticator.get_token()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }

                log_message(
                    f"Token used for request: {self.authenticator.get_token()}", level="DEBUG")
                response = requests.put(
                    url, headers=headers, json=payload, timeout=30)

                if response.status_code in {200, 204}:
                    log_message(
                        f"Successfully sent {len(payload)} records to {url}", level="DEBUG")
                    return True

                elif response.status_code == 429:
                    retry_after = int(response.headers.get(
                        "Retry-After", retry_delay))
                    log_message(
                        f"Rate limited. Retrying after {retry_after} seconds.", level="WARNING")
                    time.sleep(retry_after)
                    retry_delay *= 2

                elif response.status_code == 401:
                    log_message(
                        "Authentication failed. Getting new token on next attempt.", level="WARNING")
                    time.sleep(retry_delay)
                    retry_delay *= 2

                else:
                    log_message(
                        f"Request failed. Status: {response.status_code}, Response: {response.text}", level="ERROR")
                    response.raise_for_status()

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    log_message(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}", level="WARNING")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    log_message(
                        f"Failed to send payload after {max_retries} attempts", level="ERROR")
                    return False

        return False

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        log_message("Starting the write process.", level="INFO")

        self.validate_config(config)
        if not self.authenticator:
            self.authenticator = get_authenticator(config)

        api_url = f"https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/timeseries/{config['asset_id']}/{config['aspect_id']}"

        payload = []
        last_successful_state = None
        total_records_processed = 0
        batches_sent = 0

        for message in input_messages:
            try:
                if message.type == Type.RECORD:
                    filtered_record = self.filter_qc_properties(
                        message.record.data)
                    payload.append(filtered_record)

                    if len(payload) >= self.batch_size:
                        success = self._send_payload(api_url, payload)
                        if success:
                            latest_timestamp = max(record.get(
                                "_time", "unknown") for record in payload)
                            total_records_processed += len(payload)
                            batches_sent += 1
                            log_message(
                                f"Batch {batches_sent} processed. Latest timestamp: {latest_timestamp}. Total records: {total_records_processed}",
                                level="INFO"
                            )
                            if last_successful_state:
                                yield last_successful_state
                                last_successful_state = None
                            payload.clear()
                        else:
                            if last_successful_state:
                                yield last_successful_state
                            raise ValueError(
                                f"Failed to send batch after multiple retries")

                elif message.type == Type.STATE:
                    last_successful_state = message
                    log_message(
                        f"State message received: {message.state}", level="DEBUG")

            except Exception as e:
                log_message(
                    f"Error during write process: {str(e)}", level="ERROR")
                raise

        if payload:
            success = self._send_payload(api_url, payload)
            if success:
                latest_timestamp = max(record.get("_time", "unknown")
                                       for record in payload)
                total_records_processed += len(payload)
                batches_sent += 1
                log_message(
                    f"Final batch processed. Latest timestamp: {latest_timestamp}. Total records: {total_records_processed}",
                    level="INFO"
                )
                if last_successful_state:
                    yield last_successful_state
            else:
                raise ValueError("Failed to send final batch")

        log_message(
            f"Write completed. Total records: {total_records_processed}, Batches: {batches_sent}",
            level="INFO"
        )
