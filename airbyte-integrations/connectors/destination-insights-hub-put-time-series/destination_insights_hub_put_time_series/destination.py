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


def get_authenticator(config: Mapping[str, Any]) -> OAuthAuthenticator:
    return OAuthAuthenticator(
        token_refresh_endpoint=f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token",
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )


class DestinationInsightsHubPutTimeSeries(Destination):
    def __init__(self, config: Optional[Mapping[str, Any]] = None, debug_mode: bool = False):
        super().__init__()
        self.config = config or {}
        self.authenticator = None
        self.debug_mode = debug_mode
        self.batch_size = 100
        self.session = requests.Session()

        if config:
            self.authenticator = self.get_authenticator(config)

    def log_message(self, message: str, level: str = "INFO"):
        """
        Logs a message with the specified level.

        :param message: The log message to print.
        :param level: The severity level of the message.
        """
        # Use a default log level if self.config is None or not properly initialized
        configured_level = self.config.get("log_level", "INFO").upper()
        levels = ["DEBUG", "INFO", "WARN", "ERROR"]

        # Log only if the message level is equal or higher priority than the configured level
        if levels.index(level) >= levels.index(configured_level):
            log_message = AirbyteMessage(
                type=Type.LOG,
                log=AirbyteLogMessage(level=level, message=message)
            )
            print(log_message.json())


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
                    "log_level": {"type": "string", "enum": ["DEBUG", "INFO", "WARN", "ERROR"], "default": "INFO", "description": "Set the logging level for the destination."}
                },
            }
        )

    def check(self, logger, config: dict) -> AirbyteConnectionStatus:
        try:
            self.validate_config(config)
            if not self.authenticator:
                self.authenticator = get_authenticator(config)
            if self.authenticator.check_connection():
                self.log_message(
                    "Successfully connected to the destination.", level="INFO")
                return AirbyteConnectionStatus(status=Status.SUCCEEDED)
            else:
                self.log_message(
                    "Unable to authenticate with the destination.", level="ERROR")
                return AirbyteConnectionStatus(status=Status.FAILED, message="Unable to authenticate with the destination.")
        except Exception as e:
            self.log_message(f"Connection check failed: {str(e)}", level="ERROR")
            return AirbyteConnectionStatus(status=Status.FAILED, message=str(e))

    def _send_payload(self, url: str, payload: List[dict], max_retries: int = 5) -> bool:
        base_delay = 1
        max_delay = 300  # Maximum delay of 5 minutes
        last_retry_after = base_delay  # Track the last retry delay
        max_batch_size = 100  # Maximum allowed batch size
        min_batch_size = 25  # Minimum allowed batch size
        last_successful_state = None  # Track the last successfully written state

        for attempt in range(max_retries):
            try:
                token = self.authenticator.get_token()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }

                response = requests.put(url, headers=headers, json=payload, timeout=30)
                
                # Log detailed response for debugging
                self.log_message(f"Request Headers: {headers}", level="DEBUG")
                self.log_message(f"Request Body: {payload}", level="DEBUG")
                self.log_message(f"Response Headers: {response.headers}", level="DEBUG")
                self.log_message(f"Response Body: {response.text}", level="DEBUG")

                if response.status_code in {200, 204}:
                    # Restore batch size gradually to the max allowed (100)
                    if self.batch_size < max_batch_size:
                        self.batch_size = min(max_batch_size, self.batch_size + 25)
                        self.log_message(
                            f"Batch processed successfully. Increasing batch size to {self.batch_size}.",
                            level="INFO"
                        )
                    # Update last successful state
                    last_successful_state = {"timestamp": max(record["_time"] for record in payload)}
                    return True

                elif response.status_code == 429:
                    # Handle Retry-After header or fallback
                    retry_after_raw = response.headers.get("Retry-After", None)
                    retry_after = min(
                        max_delay,
                        int(retry_after_raw) if retry_after_raw and retry_after_raw.isdigit() else last_retry_after * 2
                    )
                    last_retry_after = retry_after  # Update the last retry delay
                    self.log_message(f"Rate limited (429). Retry-After Header: {retry_after_raw}. Calculated retry delay: {retry_after} seconds.", level="WARN")

                    # Reduce batch size on repeated rate limits, with minimum cap
                    if attempt > 1:
                        self.batch_size = max(min_batch_size, self.batch_size // 2)
                        self.log_message(f"Reducing batch size to {self.batch_size} due to repeated rate limits.", level="WARN")

                    time.sleep(retry_after)

                elif response.status_code == 401:
                    self.log_message("Authentication failed. Refreshing token and retrying.", level="WARN")
                    # Delay only for authentication recovery
                    time.sleep(base_delay)

                else:
                    # Log the request and response for other failures
                    self.log_message(
                        f"Request failed. Status: {response.status_code}, Response: {response.text}",
                        level="ERROR"
                    )
                    self.log_message(
                        f"Request URL: {url}, Headers: {headers}, Payload: {payload[:3]}... ({len(payload)} records total)",
                        level="ERROR"
                    )
                    if attempt < max_retries - 1:
                        # Apply exponential backoff for other errors
                        time.sleep(min(base_delay * (2 ** attempt), max_delay))
                    else:
                        response.raise_for_status()

            except requests.exceptions.RequestException as e:
                self.log_message(
                    f"Request failed (attempt {attempt + 1}/{max_retries}): {str(e)}",
                    level="WARN"
                )
                self.log_message(
                    f"Request URL: {url}, Headers: {headers}, Payload: {payload[:3]}... ({len(payload)} records total)",
                    level="ERROR"
                )
                if attempt < max_retries - 1:
                    # Apply exponential backoff for network issues
                    time.sleep(min(base_delay * (2 ** attempt), max_delay))
                else:
                    return False

        """ 
        Experimental feature: Emit the last successful state before exiting

        # Graceful exit if retries are exceeded
        self.log_message("Retries exceeded. Emitting state and exiting gracefully.", level="WARN")
        if last_successful_state:
            # Emit the last successful state before exiting
            self.log_message(f"Last successful state: {last_successful_state}", level="INFO")
            #yield AirbyteMessage(type=Type.STATE, state=last_successful_state)
        """
        return False

    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        self.log_message("Starting the write process.", level="INFO")

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
                            self.log_message(
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
                    self.log_message(
                        f"State message received: {message.state}", level="DEBUG")

            except Exception as e:
                self.log_message(
                    f"Error during write process: {str(e)}", level="ERROR")
                raise

        if payload:
            success = self._send_payload(api_url, payload)
            if success:
                latest_timestamp = max(record.get("_time", "unknown")
                                       for record in payload)
                total_records_processed += len(payload)
                batches_sent += 1
                self.log_message(
                    f"Final batch processed. Latest timestamp: {latest_timestamp}. Total records: {total_records_processed}",
                    level="INFO"
                )
                if last_successful_state:
                    yield last_successful_state
            else:
                raise ValueError("Failed to send final batch")

        self.log_message(
            f"Write completed. Total records: {total_records_processed}, Batches: {batches_sent}",
            level="INFO"
        )
