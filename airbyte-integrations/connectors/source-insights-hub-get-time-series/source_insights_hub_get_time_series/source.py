import time
import requests
import logging
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import ConnectorSpecification

from .utils.oauth_authenticator import OAuthAuthenticator

logger = logging.getLogger("airbyte")
logger.setLevel(logging.DEBUG)


class MachineMetricsStream(HttpStream, IncrementalMixin):
    url_base = "https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/"
    cursor_field = "_time"
    primary_key = "_time"
    name = "machine_metrics"

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(
            authenticator=OAuthAuthenticator(
                token_refresh_endpoint=f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token",
                client_id=config["client_id"],
                client_secret=config["client_secret"],
            )
        )
        self.asset_id = config["asset_id"]
        self.aspect_id = config["aspect_id"]
        self.start_date = config["start_date"]
        self._state = {}

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "_time": {
                    "type": "string",
                    "format": "date-time"
                }
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
        """
        Return the latest state by comparing the cursor value in the latest record with the current state.
        """
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

    def next_page_token(
        self, response: requests.Response
    ) -> Optional[Mapping[str, Any]]:
        records = response.json()
        if not records:
            return None
        last_record = records[-1]
        if self.cursor_field in last_record:
            token = {self.cursor_field: last_record[self.cursor_field]}
            logger.info(
                f"Requesting next page with token: {token[self.cursor_field]}")
            return token
        return None

    def request_params(
        self, stream_state=None, next_page_token=None, **kwargs
    ) -> MutableMapping[str, Any]:
        params = {"limit": 2000}
        if next_page_token:
            params["from"] = next_page_token[self.cursor_field]
        elif stream_state and stream_state.get(self.cursor_field):
            logger.info(
                f"Performing incremental sync. Using 'from' value from state: {stream_state[self.cursor_field]}")
            params["from"] = stream_state[self.cursor_field]
        else:
            logger.info(
                f"Starting full sync. Using start_date: {self.start_date}")
            params["from"] = self.start_date
        return params

    def read_records(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        logger.info("Starting to read records.")
        try:
            for record in super().read_records(**kwargs):
                # Update the state for each record
                self.state = self.get_updated_state(self.state, record)
                yield record
        except Exception as e:
            logger.error(f"Error reading records: {e}", exc_info=True)
            raise
        finally:
            logger.info("Finished reading records.")

    def parse_response(
        self, response: requests.Response, **kwargs
    ) -> Iterable[Mapping]:
        """
        Parse the HTTP response and log request/response details where necessary.
        """
        logger.info(f"Request URL: {response.request.url}")
        logger.info(f"Response Status Code: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"Non-200 Response. URL: {response.request.url}")
            logger.error(f"Headers: {response.request.headers}")
            logger.error(f"Request Body: {response.request.body}")
            logger.error(f"Response Content: {response.text}")
            response.raise_for_status()

        return response.json()


class SourceInsightsHubGetTimeSeries(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            stream = MachineMetricsStream(config)
            next(stream.read_records(sync_mode="full_refresh"))
            return True, None
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
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
