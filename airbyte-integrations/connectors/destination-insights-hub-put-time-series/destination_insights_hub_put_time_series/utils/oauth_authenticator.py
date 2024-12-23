import threading
import requests
import time
import logging
from typing import Optional
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

# Use Airbyte's logging
logger = logging.getLogger("airbyte")


class OAuthAuthenticator(TokenAuthenticator):
    def __init__(self, token_refresh_endpoint: str, client_id: str, client_secret: str):
        self.token_refresh_endpoint = token_refresh_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        self._lock = threading.Lock()  # Thread-safe caching
        super().__init__(token=self.get_token())

    def get_token(self) -> str:
        """Retrieve a valid token, refreshing it if necessary."""
        with self._lock:  # Ensure only one thread updates the token at a time
            # Check if the cached token is still valid
            if self.access_token and self.token_expires_at and self.token_expires_at > time.time():
                logger.debug(
                    f"Using cached token. Expires at {self.token_expires_at}, current time: {time.time()}.")
                return self.access_token

            # Token is expired or not set; refresh it
            logger.debug("Refreshing token...")
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

                # Retrieve the token and calculate its expiry with a buffer
                expires_in = token_response.get("expires_in")
                if expires_in is None:
                    raise ValueError(
                        "Token response is missing 'expires_in' property.")

                self.access_token = token_response["access_token"]
                # Subtract a 5-minute (300-second) buffer from the token's expiry time
                self.token_expires_at = time.time() + expires_in - 300
                logger.debug(
                    f"Token refreshed successfully. New token expires at {self.token_expires_at} "
                    f"(in {expires_in} seconds, buffer applied)."
                )
                return self.access_token
            except requests.RequestException as e:
                logger.error(
                    f"Token refresh failed: {e.response.text if hasattr(e, 'response') else str(e)}")
                raise ValueError(f"Token retrieval failed: {str(e)}") from e

    def check_connection(self) -> bool:
        """Checks if the connection can be successfully established by retrieving a token."""
        try:
            self.get_token()  # Attempt to fetch or validate the token
            logger.info("Connection check succeeded.")
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            return False
