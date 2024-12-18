import time
import requests
import re
import logging
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models import ConnectorSpecification

logging.getLogger("urllib3").setLevel(logging.DEBUG)


class OAuthAuthenticator(TokenAuthenticator):
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
        self._access_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None

        # Call get_access_token to initialize the token
        super().__init__(self.get_access_token())

    def get_access_token(self) -> str:
        # Check if current token is valid
        if self._access_token and self._token_expires_at and self._token_expires_at > time.time():
            return self._access_token

        # Refresh token if needed
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

        # Extract access token and expiration
        self._access_token = token_response[self.access_token_name]
        expires_in = token_response.get(self.expires_in_name, 3600)
        self._token_expires_at = time.time() + expires_in - 60

        # Update the parent class token
        self.token = self._access_token

        return self._access_token

    def refresh_access_token(self) -> None:
        # Force token refresh
        self._access_token = None
        self._token_expires_at = None
        self.get_access_token()


def get_authenticator(config: dict) -> OAuthAuthenticator:
    """
    Create an OAuth authenticator for Insights Hub
    """
    token_refresh_endpoint = f"https://{config['tenant_id']}.piam.eu1.mindsphere.io/oauth/token"
    return OAuthAuthenticator(
        token_refresh_endpoint=token_refresh_endpoint,
        client_id=config["client_id"],
        client_secret=config["client_secret"],
    )


class InsightsHubGetTimeSeriesStream(HttpStream):
    url_base = "https://gateway.eu1.mindsphere.io/api/iottimeseries/v3/"
    primary_key = "_time"
    cursor_field = "_time"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(authenticator=get_authenticator(config), *args, **kwargs)
        self.asset_id = config["asset_id"]
        self.aspect_id = config["aspect_id"]
        self.limit = config.get("limit", 2000)
        self.start_date = config.get("start_date")

    def request_params(self, stream_state=None, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        params = {"limit": self.limit}

        if next_page_token and "from" in next_page_token:
            params["from"] = next_page_token["from"]
            self.logger.debug(
                f"Using next_page_token for from: {params['from']}")
        elif stream_state and self.cursor_field in stream_state and stream_state[self.cursor_field]:
            # Make sure we have both a state and a cursor value
            params["from"] = stream_state[self.cursor_field]
            self.logger.debug(f"Using stream_state for from: {params['from']}")
        else:
            params["from"] = self.start_date
            self.logger.debug(f"Using start_date for from: {params['from']}")

        self.logger.debug(f"Final params: {params}")
        return params

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to handle state correctly
        """
        self.logger.debug(
            f"Updating state - Current: {current_stream_state}, Latest record time: {latest_record.get(self.cursor_field)}")

        current_state_value = current_stream_state.get(self.cursor_field)
        latest_value = latest_record.get(self.cursor_field)

        if not current_state_value:
            self.logger.debug(
                f"No current state, using latest: {latest_value}")
            return {self.cursor_field: latest_value}

        if latest_value and latest_value > current_state_value:
            self.logger.debug(f"Found newer state: {latest_value}")
            return {self.cursor_field: latest_value}

        self.logger.debug(f"Keeping current state: {current_state_value}")
        return current_stream_state

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        records = response.json()
        if not records:
            self.logger.debug(
                "No records found in response; pagination ended.")
            return None

        last_record_time = records[-1].get("_time")
        if last_record_time:
            self.logger.debug(f"Next page token generated: {last_record_time}")
            return {"from": last_record_time}
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for record in response.json():
            yield record


class MachineMetricsStream(InsightsHubGetTimeSeriesStream):
    name = "machine_metrics"  # Aligns with the catalog name

    def path(self, **kwargs) -> str:
        return f"timeseries/{self.asset_id}/{self.aspect_id}"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "type": "object",
            "required": ["_time"],
            "properties": {
                "_time": {"type": "string", "format": "date-time"},
            },
            "additionalProperties": True,
        }


class SourceInsightsHubGetTimeSeries(AbstractSource):

    def spec(self, logger=None) -> ConnectorSpecification:
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
                    "start_date"  # Added start_date as required
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
                    },
                    "limit": {
                        "type": "integer",
                        "title": "Limit",
                        "description": "Response Page Limit",
                        "default": 2000,
                        "minimum": 1,
                        "maximum": 10000,
                        "order": 5
                    },
                    "start_date": {
                        "type": "string",
                        "title": "Start Date",
                        "description": "Start date for timeseries data in UTC (YYYY-MM-DDTHH:MM:SSZ)",
                        "format": "date-time",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                        "order": 6
                    }
                }
            }

        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            auth = get_authenticator(config)
            token = auth.get_access_token()
            logger.info(f"Successfully retrieved access token")
            return True, None
        except Exception as e:
            return False, f"Connection failed: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [MachineMetricsStream(config=config)]
