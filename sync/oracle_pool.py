"""Thin wrapper around cx_Oracle's session pool."""
from __future__ import annotations

import contextlib
import logging
import threading

log = logging.getLogger(__name__)

try:
    import cx_Oracle
except ImportError:  # pragma: no cover - tests can mock this out
    cx_Oracle = None  # type: ignore[assignment]


class OraclePool:
    def __init__(self, user: str, password: str, dsn: str, min_sessions: int = 2,
                 max_sessions: int = 6):
        if cx_Oracle is None:
            raise RuntimeError("cx_Oracle is not installed")
        self._pool = cx_Oracle.SessionPool(
            user=user, password=password, dsn=dsn,
            min=min_sessions, max=max_sessions, increment=1,
            threaded=True, getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT,
        )
        self._lock = threading.Lock()

    @contextlib.contextmanager
    def connection(self):
        conn = self._pool.acquire()
        try:
            yield conn
        finally:
            try:
                self._pool.release(conn)
            except Exception:
                log.exception("failed to release oracle connection")

    def close(self) -> None:
        try:
            self._pool.close(force=True)
        except Exception:
            log.exception("failed to close oracle pool")
