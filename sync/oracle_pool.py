"""Thin wrapper around cx_Oracle's session pool with infinite retry on connect."""
from __future__ import annotations

import contextlib
import logging
import threading

from .connectivity import escalating_backoff, ping_internet, wait_for_internet

log = logging.getLogger(__name__)

try:
    import cx_Oracle
except ImportError:  # pragma: no cover - tests can mock this out
    cx_Oracle = None  # type: ignore[assignment]


class OraclePool:
    def __init__(self, user: str, password: str, dsn: str, min_sessions: int = 2,
                 max_sessions: int = 6,
                 stop_event: threading.Event | None = None):
        if cx_Oracle is None:
            raise RuntimeError("cx_Oracle is not installed")
        self._user = user
        self._password = password
        self._dsn = dsn
        self._min = min_sessions
        self._max = max_sessions
        self._stop = stop_event
        self._lock = threading.Lock()
        self._pool = self._create_pool_with_retry()

    def _create_pool_with_retry(self):
        """Create the session pool, retrying forever if Oracle is unreachable."""
        attempt = 0
        while True:
            attempt += 1
            try:
                pool = cx_Oracle.SessionPool(
                    user=self._user, password=self._password, dsn=self._dsn,
                    min=self._min, max=self._max, increment=1,
                    threaded=True, getmode=cx_Oracle.SPOOL_ATTRVAL_WAIT,
                )
                if attempt > 1:
                    log.info("oracle pool created after %d attempts", attempt)
                return pool
            except Exception as e:
                log.warning(
                    "oracle pool creation failed (attempt %d): %s", attempt, e,
                )
                delay = escalating_backoff(attempt)
                if self._stop is not None:
                    if self._stop.wait(delay):
                        raise RuntimeError("shutdown during Oracle pool creation")
                else:
                    import time
                    time.sleep(delay)

    @contextlib.contextmanager
    def connection(self):
        """Acquire a connection, retrying forever on connection errors."""
        attempt = 0
        while True:
            attempt += 1
            try:
                conn = self._pool.acquire()
                break
            except Exception as e:
                log.warning(
                    "oracle acquire failed (attempt %d): %s", attempt, e,
                )
                delay = escalating_backoff(attempt)
                import time
                time.sleep(delay)
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
