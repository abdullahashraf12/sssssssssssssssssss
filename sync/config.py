"""Centralised configuration. Reads from environment; tolerates a .env file."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_env_file(Path(__file__).resolve().parent.parent / ".env")


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _read_refresh_token() -> str:
    explicit = os.environ.get("ZOHO_REFRESH_TOKEN")
    if explicit:
        return explicit.strip()
    path = os.environ.get(
        "ZOHO_REFRESH_TOKEN_FILE",
        str(Path(__file__).resolve().parent.parent / "zoho_refresh_token.txt"),
    )
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read().strip()


@dataclass(frozen=True)
class Config:
    oracle_user: str
    oracle_pass: str
    oracle_dsn: str
    zoho_account_owner: str
    zoho_app: str
    zoho_form_items: str
    zoho_form_branches: str
    zoho_client_id: str
    zoho_client_secret: str
    zoho_refresh_token: str
    zoho_token_url: str
    zoho_api_base: str
    max_attempts: int
    realtime_batch: int


def load() -> Config:
    return Config(
        oracle_user=os.environ.get("ORACLE_USER", "test"),
        oracle_pass=os.environ.get("ORACLE_PASS", "test"),
        oracle_dsn=os.environ.get("ORACLE_DSN", "localhost:1521/orcl"),
        zoho_account_owner=os.environ.get("ZOHO_ACCOUNT_OWNER", "alpha1.abdullah771"),
        zoho_app=os.environ.get("ZOHO_APP", "carton"),
        zoho_form_items=os.environ.get("ZOHO_FORM_ITEMS", "Items_Data"),
        zoho_form_branches=os.environ.get("ZOHO_FORM_BRANCHES", "Branches_Codes"),
        zoho_client_id=_require("ZOHO_CLIENT_ID"),
        zoho_client_secret=_require("ZOHO_CLIENT_SECRET"),
        zoho_refresh_token=_read_refresh_token(),
        zoho_token_url=os.environ.get(
            "ZOHO_TOKEN_URL", "https://accounts.zoho.com/oauth/v2/token"
        ),
        zoho_api_base=os.environ.get(
            "ZOHO_API_BASE", "https://creatorapp.zoho.com/api/v2"
        ),
        max_attempts=int(os.environ.get("SYNC_MAX_ATTEMPTS", "5")),
        realtime_batch=int(os.environ.get("SYNC_REALTIME_BATCH", "10")),
    )
