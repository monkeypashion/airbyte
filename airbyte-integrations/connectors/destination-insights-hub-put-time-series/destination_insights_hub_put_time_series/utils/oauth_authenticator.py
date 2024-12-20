import requests
import time
from typing import Optional
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


class OAuthAuthenticator(TokenAuthenticator):
    def __init__(
        self,
        token_refresh_endpoint: str,
        client_id: str,
        client_secret: str,
    ):
        self.token_refresh_endpoint = token_refresh_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        super().__init__(token=self.get_token())

    def get_token(self) -> str:
        if self.access_token and self.token_expires_at and self.token_expires_at > time.time():
            return self.access_token

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
            self.access_token = token_response["access_token"]
            self.token_expires_at = time.time() + token_response.get("expires_in",
                                                                     1800)  # Default to 30 minutes if not provided
            return self.access_token
        except requests.RequestException as e:
            raise ValueError(f"Token retrieval failed: {str(e)}") from e

    def check_connection(self) -> bool:
        try:
            self.get_token()
            return True
        except Exception:
            return False
