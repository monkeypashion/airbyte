import time
import requests
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import ConnectorSpecification, AirbyteMessage, AirbyteLogMessage, Type
from .utils.oauth_authenticator import OAuthAuthenticator


def log_message(message: str, level: str = "INFO"):
    log_message = AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=level, message=message)
    )
    print(log_message.json())  # Send to stdout for Airbyte to parse


def get_authenticator(config: Mapping[str, Any]) -> OAuthAuthenticator:
    """
    Creates and returns an OAuthAuthenticator instance.
    """
    return OAuthAuthenticator(
        token_refresh_endpoint=f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token",
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )


class MachineMetricsStream(HttpStream, IncrementalMixin):
    url_base = "https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/"
    cursor_field = "_time"
    primary_key = "_time"
    name = "machine_metrics"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        self.authenticator = get_authenticator(config)
        self.asset_id = config["asset_id"]
        self.aspect_id = config["aspect_id"]
        self.start_date = config["start_date"]
        self._state = {}

        super().__init__(authenticator=self.authenticator)

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "_time": {"type": "string", "format": "date-time"}
            },
            "additionalProperties": True,
            "required": ["_time"]
        }

    @property
    def state(self) -> Mapping[str, Any]:
        if self._state:
            return {self.cursor_field: self._state.get(self.cursor_field)}
        return {}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._state = value or {}

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        latest_cursor_value = latest_record.get(self.cursor_field)
        current_cursor_value = (
            current_stream_state.get(self.cursor_field)
            if current_stream_state
            else None
        )
        if latest_cursor_value and (
            not current_cursor_value or latest_cursor_value > current_cursor_value
        ):
            return {self.cursor_field: latest_cursor_value}
        return current_stream_state or {}

    def path(self, **kwargs) -> str:
        return f"timeseries/{self.asset_id}/{self.aspect_id}"

    def request_headers(self, **kwargs) -> Mapping[str, Any]:
        token = self.authenticator.get_token()
        headers = super().request_headers(**kwargs)
        headers["Authorization"] = f"Bearer {token}"
        log_message(f"Token used for request: {token}", level="DEBUG")
        return headers

    def request_params(
        self, stream_state=None, next_page_token=None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {"limit": 2000}
        if next_page_token:
            params["from"] = next_page_token[self.cursor_field]
        elif stream_state and stream_state.get(self.cursor_field):
            log_message(
                f"Incremental sync from: {stream_state[self.cursor_field]}",
                level="DEBUG"
            )
            params["from"] = stream_state[self.cursor_field]
        else:
            log_message(
                f"Full sync from start_date: {self.start_date}", level="DEBUG")
            params["from"] = self.start_date
        return params

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        records = response.json()
        if not records:
            return None

        last_record = records[-1]
        if self.cursor_field in last_record:
            token = {self.cursor_field: last_record[self.cursor_field]}
            log_message(
                f"Next page token: {token[self.cursor_field]}", level="DEBUG")
            return token

        return None

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        try:
            log_headers = dict(response.request.headers)
            if 'Authorization' in log_headers:
                log_headers['Authorization'] = 'Bearer [REDACTED]'
            log_message(f"Request headers: {log_headers}", level="DEBUG")
            log_message(f"Request URL: {response.request.url}", level="DEBUG")

            response.raise_for_status()

            records = response.json()

            if isinstance(records, list):
                log_message(
                    f"Parsed {len(records)} records from response", level="DEBUG")
                return records
            else:
                log_message(
                    "Unexpected response format: not a list", level="ERROR")
                raise ValueError("Response is not a list of records")

        except requests.HTTPError as e:
            log_message(
                f"HTTP error: {e.response.status_code} - {e.response.text}",
                level="ERROR"
            )
            raise
        except Exception as e:
            log_message(f"Error parsing response: {str(e)}", level="ERROR")
            raise

    def read_records(self, sync_mode=None, stream_state=None, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Fetch and yield records from the API, managing state and logging appropriately.
        """
        log_message("Starting to read records", level="DEBUG")
        retry_attempts = 3

        for attempt in range(retry_attempts):
            try:
                for record in super().read_records(sync_mode=sync_mode, stream_state=stream_state, **kwargs):
                    # Update the stream state for incremental sync
                    self.state = self.get_updated_state(self.state, record)
                    yield record  # Yield the record to the Airbyte platform
                break
            except requests.HTTPError as e:
                if e.response.status_code == 401 and attempt < retry_attempts - 1:
                    log_message(
                        "Token expired. Refreshing and retrying...", level="WARNING")
                    self.authenticator.get_token()  # Force token refresh
                    continue
                else:
                    log_message(
                        f"Error reading records: {str(e)}", level="ERROR")
                    raise
        log_message("Finished reading records", level="DEBUG")


class SourceInsightsHubGetTimeSeries(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        Verifies the connection by attempting to fetch a token and make a simple request.
        """
        try:
            # Use the get_authenticator function
            authenticator = get_authenticator(config)
            if authenticator.check_connection():
                log_message("Successfully authenticated", level="INFO")
                # Perform a simple data fetch to ensure API access
                stream = MachineMetricsStream(config)
                next(stream.read_records(sync_mode="full_refresh"))
                return True, None
            else:
                log_message("Authentication failed", level="ERROR")
                return False, "Authentication failed"
        except Exception as e:
            log_message(f"Connection check failed: {str(e)}", level="ERROR")
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [MachineMetricsStream(config)]

    def spec(self, *args, **kwargs) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.com/integrations/sources/insights-hub-get-time-series",
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Insights Hub Get Timeseries",
                "type": "object",
                "required": [
                    "asset_id",
                    "aspect_id",
                    "client_id",
                    "client_secret",
                    "tenant_id",
                    "start_date",
                ],
                "properties": {
                    "asset_id": {
                        "type": "string",
                        "title": "Asset ID",
                        "description": "Insights Hub Asset ID",
                    },
                    "aspect_id": {
                        "type": "string",
                        "title": "Aspect ID",
                        "description": "Insights Hub Aspect ID",
                    },
                    "client_id": {
                        "type": "string",
                        "title": "Client ID",
                        "description": "OAuth Client ID",
                        "airbyte_secret": True,
                    },
                    "client_secret": {
                        "type": "string",
                        "title": "Client Secret",
                        "description": "OAuth Client Secret",
                        "airbyte_secret": True,
                    },
                    "tenant_id": {
                        "type": "string",
                        "title": "Tenant ID",
                        "description": "Tenant ID",
                    },
                    "start_date": {
                        "type": "string",
                        "title": "Start Date",
                        "description": "Start date in UTC (YYYY-MM-DDTHH:MM:SSZ)",
                        "format": "date-time",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                    },
                },
            },
        )
