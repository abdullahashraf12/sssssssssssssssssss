"""Always-on worker that retries DEAD events.

Worker D runs as a single background thread from service startup.  It
continuously polls ``SYNC_EVENTS`` for rows with ``status = 'DEAD'``,
resets them to ``'NEW'`` (with ``attempts = 0``), so that the normal
realtime workers can pick them up again.

It never blocks Worker R or Worker B — completely independent.
"""
from __future__ import annotations

import logging
import threading
from typing import Any

log = logging.getLogger(__name__)

# How often to poll for DEAD events (seconds).
_POLL_INTERVAL = 30.0


class DeadRetryWorker:
    """Single always-on thread that resurrects DEAD events."""

    def __init__(
        self,
        pool,
        poll_interval: float = _POLL_INTERVAL,
        stop_event: threading.Event | None = None,
    ):
        self._pool = pool
        self._poll_interval = poll_interval
        self._stop = stop_event or threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        log.info("dead-retry worker started (poll every %.0f s)", self._poll_interval)
        while not self._stop.is_set():
            try:
                count = self._resurrect_dead()
                if count > 0:
                    log.info("dead-retry: reset %d DEAD event(s) → NEW", count)
            except Exception:
                log.exception("dead-retry: error during poll cycle")
            # Sleep until next poll (or until stop is requested)
            if self._stop.wait(self._poll_interval):
                break
        log.info("dead-retry worker stopped")

    def _resurrect_dead(self) -> int:
        """Reset all DEAD events back to NEW with attempts=0."""
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE SYNC_EVENTS "
                "SET status = 'NEW', attempts = 0, next_attempt_at = NULL "
                "WHERE status = 'DEAD'"
            )
            count = cursor.rowcount
            conn.commit()
            return count
