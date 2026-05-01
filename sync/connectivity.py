"""Shared internet-connectivity check and escalating-backoff helpers.

Used by token_manager, zoho_client, oracle_pool, and workers to implement
the production infinite-retry policy:

    1 s → 3 s → 10 s → 15 s → 1 min → +5 min each step (forever)
"""
from __future__ import annotations

import logging
import threading
import time

import requests

log = logging.getLogger(__name__)

# The fixed portion of the backoff ladder before it switches to
# "+5 minutes each step".
_FIXED_STEPS: tuple[float, ...] = (1.0, 3.0, 10.0, 15.0, 60.0)


def escalating_backoff(attempt: int) -> float:
    """Return the wait time (seconds) for *attempt* (1-based).

    Steps 1-5 follow the fixed ladder.
    From step 6 onward: 1 min + 5 min × (step - 5).
    """
    if attempt <= len(_FIXED_STEPS):
        return _FIXED_STEPS[attempt - 1]
    extra_steps = attempt - len(_FIXED_STEPS)
    return 60.0 + 300.0 * extra_steps  # 60s base + 5 min per extra step


def ping_internet(timeout: float = 5.0) -> bool:
    """Return *True* if the machine can reach the public internet."""
    try:
        requests.get("https://google.com", timeout=timeout, allow_redirects=True)
        return True
    except Exception:
        return False


def wait_for_internet(
    stop_event: threading.Event | None = None,
    label: str = "internet",
) -> None:
    """Block until :func:`ping_internet` succeeds.

    Uses the escalating backoff schedule (infinite — no max).
    Respects *stop_event* so the service can still shut down cleanly.
    """
    attempt = 0
    while True:
        attempt += 1
        if ping_internet():
            if attempt > 1:
                log.info("%s: connectivity restored after %d attempts", label, attempt)
            return
        delay = escalating_backoff(attempt)
        log.warning(
            "%s: no connectivity (attempt %d), retrying in %.0f s …",
            label, attempt, delay,
        )
        if stop_event is not None:
            if stop_event.wait(delay):
                raise InterruptedError(f"{label}: shutdown during connectivity wait")
        else:
            time.sleep(delay)
