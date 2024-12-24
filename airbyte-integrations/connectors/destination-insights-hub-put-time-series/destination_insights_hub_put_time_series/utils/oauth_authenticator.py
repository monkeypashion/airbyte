import threading
import requests
import time
from typing import Optional, Mapping, Any
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from airbyte_cdk.models import AirbyteMessage, AirbyteLogMessage, Type


def log_message(message: str, level: str = "INFO"):
    log_message = AirbyteMessage(
        type=Type.LOG,
        log=AirbyteLogMessage(level=level, message=message)
    )
    print(log_message.json())  # Send to stdout for Airbyte to parse


class OAuthAuthenticator(TokenAuthenticator):
    def __init__(self, token_refresh_endpoint: str, client_id: str, client_secret: str):
        self.token_refresh_endpoint = token_refresh_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        self._lock = threading.Lock()
        self._refresh_in_progress = threading.Event()

        # Initialize with empty token, we'll get it on first use
        super().__init__(token="")

    def get_auth_header(self) -> Mapping[str, Any]:
        """Override to provide the auth header with the current token."""
        token = self.get_token()
        return {"Authorization": f"Bearer {token}"}

    def get_token(self) -> str:
        """Retrieve a valid token, refreshing it if necessary."""
        with self._lock:
            current_time = time.time()

            # Check if token is still valid with a 28-minute buffer
            if (self.access_token and self.token_expires_at and
                    self.token_expires_at > (current_time + 1680)):  # 28-minute buffer
                return self.access_token

            # Prevent multiple simultaneous refresh attempts
            if self._refresh_in_progress.is_set():
                log_message(
                    "Token refresh already in progress, waiting...", level="DEBUG")
                self._refresh_in_progress.wait()
                return self.access_token

            try:
                self._refresh_in_progress.set()
                return self._refresh_token()
            finally:
                self._refresh_in_progress.clear()

    def _refresh_token(self) -> str:
        """Internal method to refresh the token."""
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

            token_data = response.json()
            expires_in = token_data.get("expires_in")

            if not expires_in:
                raise ValueError("Token response missing 'expires_in' field")

            self.access_token = token_data["access_token"]
            self.token_expires_at = time.time() + float(expires_in)

            log_message(
                f"Token refreshed successfully. Expires in {expires_in} seconds (effective expiry in {expires_in - 1680} seconds with buffer)",
                level="DEBUG"
            )
            return self.access_token

        except requests.ConnectionError as e:
            log_message(
                f"Failed to connect to auth server: {str(e)}", level="ERROR")
            raise ValueError(
                f"Failed to connect to auth server: {str(e)}") from e
        except requests.Timeout as e:
            log_message(
                f"Authentication request timed out: {str(e)}", level="ERROR")
            raise ValueError(
                f"Authentication request timed out: {str(e)}") from e
        except requests.RequestException as e:
            error_message = e.response.text if hasattr(
                e, "response") and e.response else str(e)
            log_message(
                f"Token refresh failed: {error_message}", level="ERROR")
            raise ValueError(f"Token refresh failed: {error_message}") from e
        except KeyError as e:
            log_message(
                f"Unexpected token response format: {str(e)}", level="ERROR")
            raise ValueError(
                f"Unexpected token response format: {str(e)}") from e
        except Exception as e:
            log_message(
                f"Unexpected error during token refresh: {str(e)}", level="ERROR")
            raise ValueError(
                f"Unexpected error during token refresh: {str(e)}") from e

    def check_connection(self) -> bool:
        """Verify authentication by attempting to fetch a token."""
        try:
            self.get_token()
            log_message("Successfully authenticated.", level="INFO")
            return True
        except Exception as e:
            log_message(f"Connection check failed: {str(e)}", level="ERROR")
            return False
