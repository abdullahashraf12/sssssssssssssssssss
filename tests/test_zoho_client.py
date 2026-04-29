from unittest.mock import MagicMock

import pytest
import requests

from sync.rate_limiter import IntervalLimiter
from sync.zoho_client import ZohoClient, ZohoError


class FakeTokens:
    def __init__(self):
        self.value = "TOKEN-1"
        self.invalidate_count = 0
        self.force_count = 0

    def get(self, force_refresh: bool = False):
        if force_refresh:
            self.force_count += 1
            self.value = "TOKEN-2"
        return self.value

    def invalidate(self):
        self.invalidate_count += 1


def _resp(sc, body=None, headers=None):
    r = MagicMock()
    r.status_code = sc
    r.headers = headers or {}
    r.content = b"{}" if body is not None else b""
    r.json.return_value = body or {}
    r.text = str(body)
    return r


def _client(session, max_attempts=3):
    tokens = FakeTokens()
    limiter = IntervalLimiter(0.0)
    sleeps: list[float] = []
    c = ZohoClient(
        "owner", "carton", "https://api.example.com/api/v2",
        tokens, limiter, session=session, max_attempts=max_attempts,
        sleep_func=sleeps.append,
    )
    return c, tokens, sleeps


def test_add_record_success_201_returns_id():
    session = MagicMock()
    session.request.return_value = _resp(201, {"data": {"ID": "999"}})
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {"Item_Code": "X"},
                        priority=IntervalLimiter.REALTIME) == "999"


def test_add_record_success_200_also_accepted():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": [{"ID": "abc"}]})
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "abc"


def test_validation_400_raises_zoho_error():
    session = MagicMock()
    session.request.return_value = _resp(400, {"error": "bad"})
    c, _, _ = _client(session)
    with pytest.raises(ZohoError) as exc:
        c.add_record("Items_Data", {}, priority=0)
    assert exc.value.status_code == 400


def test_401_triggers_token_refresh_and_retries():
    session = MagicMock()
    session.request.side_effect = [
        _resp(401, {"error": "auth"}),
        _resp(201, {"data": {"ID": "ok"}}),
    ]
    c, tokens, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "ok"
    assert tokens.invalidate_count == 1
    assert tokens.force_count == 1


def test_429_respects_retry_after_and_slows_limiter():
    session = MagicMock()
    session.request.side_effect = [
        _resp(429, {}, headers={"Retry-After": "0"}),
        _resp(201, {"data": {"ID": "1"}}),
    ]
    c, _, sleeps = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "1"
    assert sleeps and sleeps[0] == 0.0  # honored Retry-After


def test_5xx_retries_then_succeeds():
    session = MagicMock()
    session.request.side_effect = [
        _resp(503),
        _resp(503),
        _resp(201, {"data": {"ID": "1"}}),
    ]
    c, _, _ = _client(session)
    assert c.add_record("Items_Data", {}, priority=0) == "1"


def test_5xx_exhausts_attempts_and_raises():
    session = MagicMock()
    session.request.return_value = _resp(503)
    c, _, _ = _client(session, max_attempts=2)
    with pytest.raises(ZohoError):
        c.add_record("Items_Data", {}, priority=0)


def test_network_error_retries_then_raises():
    session = MagicMock()
    session.request.side_effect = requests.ConnectionError("nope")
    c, _, _ = _client(session, max_attempts=2)
    with pytest.raises(ZohoError):
        c.add_record("Items_Data", {}, priority=0)


def test_update_record_uses_patch_to_report_url():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": {}})
    c, _, _ = _client(session)
    c.update_record("Items_Report", "ID-7", {"x": 1}, priority=0)
    method, url = session.request.call_args.args
    assert method == "PATCH"
    assert url.endswith("/report/Items_Report/ID-7")


def test_delete_record_uses_delete_to_report_url():
    session = MagicMock()
    session.request.return_value = _resp(200, {"data": {}})
    c, _, _ = _client(session)
    c.delete_record("Items_Report", "ID-7", priority=0)
    method, url = session.request.call_args.args
    assert method == "DELETE"
    assert url.endswith("/report/Items_Report/ID-7")
