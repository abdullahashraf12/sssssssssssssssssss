"""Holds the current Zoho access token, refreshes it on schedule and on 401."""
from __future__ import annotations

import threading
import time

import requests


class TokenError(RuntimeError):
    pass


class TokenManager:
    REFRESH_AFTER = 50 * 60  # 50 minutes; Zoho tokens last ~60.

    def __init__(self, client_id: str, client_secret: str, refresh_token: str,
                 token_url: str, session: requests.Session | None = None,
                 time_func=time.monotonic):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._token_url = token_url
        self._session = session or requests.Session()
        self._time = time_func
        self._lock = threading.Lock()
        self._token: str | None = None
        self._fetched_at: float = 0.0

    def get(self, force_refresh: bool = False) -> str:
        with self._lock:
            if (
                force_refresh
                or self._token is None
                or self._time() - self._fetched_at >= self.REFRESH_AFTER
            ):
                self._token = self._fetch()
                self._fetched_at = self._time()
            return self._token

    def invalidate(self) -> None:
        with self._lock:
            self._token = None
            self._fetched_at = 0.0

    def _fetch(self) -> str:
        try:
            resp = self._session.post(
                self._token_url,
                params={
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "refresh_token": self._refresh_token,
                    "grant_type": "refresh_token",
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise TokenError(f"Token request failed: {e}") from e

        if resp.status_code != 200:
            raise TokenError(
                f"Token request returned {resp.status_code}: {resp.text[:300]}"
            )
        try:
            data = resp.json()
        except ValueError as e:
            raise TokenError(f"Token response was not JSON: {resp.text[:300]}") from e

        token = data.get("access_token")
        if not token:
            raise TokenError(f"Token response had no access_token: {data}")
        return token
