"""Zoho Creator API client with retry, backoff and rate limiting."""
from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Callable

import requests

from .rate_limiter import IntervalLimiter
from .token_manager import TokenManager

log = logging.getLogger(__name__)


class ZohoError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, body: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class ZohoClient:
    """One outbound API call per `limiter.interval` seconds, globally."""

    SUCCESS_CODES = (200, 201)
    MAX_ATTEMPTS_DEFAULT = 5

    def __init__(self, account_owner: str, app: str, api_base: str,
                 token_manager: TokenManager, limiter: IntervalLimiter,
                 session: requests.Session | None = None,
                 max_attempts: int = MAX_ATTEMPTS_DEFAULT,
                 sleep_func: Callable[[float], None] = time.sleep):
        self._owner = account_owner
        self._app = app
        self._base = api_base.rstrip("/")
        self._tokens = token_manager
        self._limiter = limiter
        self._session = session or requests.Session()
        self._max_attempts = max_attempts
        self._sleep = sleep_func

    # -- form / report URL helpers
    def _form_url(self, form: str) -> str:
        return f"{self._base}/{self._owner}/{self._app}/form/{form}"

    def _report_record_url(self, report: str, record_id: str) -> str:
        return f"{self._base}/{self._owner}/{self._app}/report/{report}/{record_id}"

    # -- public ops
    def add_record(self, form: str, payload: dict, priority: int) -> str:
        body = {"data": payload}
        resp = self._call("POST", self._form_url(form), body, priority=priority)
        return _extract_record_id(resp)

    def update_record(self, report: str, record_id: str, payload: dict, priority: int) -> None:
        body = {"data": payload}
        self._call("PATCH", self._report_record_url(report, record_id), body, priority=priority)

    def delete_record(self, report: str, record_id: str, priority: int) -> None:
        self._call("DELETE", self._report_record_url(report, record_id), None, priority=priority)

    # -- core retry loop
    def _call(self, method: str, url: str, body: dict | None, priority: int) -> dict:
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            self._limiter.acquire(priority=priority)
            token = self._tokens.get()
            headers = {
                "Authorization": f"Zoho-oauthtoken {token}",
                "Content-Type": "application/json",
            }
            try:
                if body is None:
                    resp = self._session.request(method, url, headers=headers, timeout=30)
                else:
                    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
                    resp = self._session.request(method, url, headers=headers,
                                                 data=data, timeout=30)
            except requests.RequestException as e:
                last_exc = e
                self._sleep_backoff(attempt)
                continue

            sc = resp.status_code
            if sc in self.SUCCESS_CODES:
                try:
                    return resp.json() if resp.content else {}
                except ValueError:
                    return {}
            if sc == 401:
                self._tokens.invalidate()
                self._tokens.get(force_refresh=True)
                continue
            if sc == 429:
                retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
                self._limiter.slow_down(factor=2.0, duration=60.0)
                self._sleep(retry_after if retry_after is not None else self._backoff_seconds(attempt))
                continue
            if 500 <= sc < 600:
                self._sleep_backoff(attempt)
                continue

            try:
                err_body = resp.json()
            except ValueError:
                err_body = resp.text
            raise ZohoError(f"Zoho {method} {url} -> {sc}", status_code=sc, body=err_body)

        raise ZohoError(
            f"Zoho {method} {url} exhausted {self._max_attempts} attempts: {last_exc}"
        )

    def _sleep_backoff(self, attempt: int) -> None:
        self._sleep(self._backoff_seconds(attempt))

    @staticmethod
    def _backoff_seconds(attempt: int) -> float:
        base = min(2 ** (attempt - 1), 30)
        return base + random.uniform(0, base * 0.25)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _extract_record_id(resp: dict) -> str:
    """Pulls the new Zoho record id out of the v2 add-record response."""
    if not isinstance(resp, dict):
        raise ZohoError(f"Zoho add-record returned non-dict: {resp!r}")
    data = resp.get("data")
    if isinstance(data, list) and data:
        candidate = data[0]
    elif isinstance(data, dict):
        candidate = data
    else:
        candidate = resp
    rid = candidate.get("ID") or candidate.get("id") or candidate.get("zc_record_id")
    if not rid:
        raise ZohoError(f"Zoho add-record missing record ID in response: {resp!r}")
    return str(rid)
