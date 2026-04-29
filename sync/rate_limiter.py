"""Single global outbound throttle: at most one Zoho call per `interval` seconds.

Two priorities are supported. When both Worker R (realtime) and Worker B
(backfill) are competing for the next slot, realtime wins. The global
spacing is preserved either way.
"""
from __future__ import annotations

import threading
import time


class IntervalLimiter:
    REALTIME = 0
    BACKFILL = 1

    def __init__(self, interval_seconds: float, time_func=time.monotonic, sleep_func=time.sleep):
        if interval_seconds < 0:
            raise ValueError("interval_seconds must be >= 0")
        self._interval = float(interval_seconds)
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._next_allowed = 0.0
        self._waiting_realtime = 0
        self._time = time_func
        self._sleep = sleep_func
        self._slowdown_until = 0.0
        self._slowdown_factor = 1.0

    @property
    def interval(self) -> float:
        return self._interval * self._effective_factor()

    def _effective_factor(self) -> float:
        if self._time() < self._slowdown_until:
            return self._slowdown_factor
        return 1.0

    def slow_down(self, factor: float = 2.0, duration: float = 60.0) -> None:
        with self._lock:
            self._slowdown_factor = max(self._slowdown_factor, factor)
            self._slowdown_until = self._time() + duration
            self._cv.notify_all()

    def acquire(self, priority: int = REALTIME) -> None:
        """Block until it is this caller's turn. Spacing-correct under contention."""
        with self._cv:
            if priority == self.REALTIME:
                self._waiting_realtime += 1
            try:
                while True:
                    now = self._time()
                    if priority == self.BACKFILL and self._waiting_realtime > 0:
                        self._cv.wait(timeout=0.05)
                        continue
                    wait_for = self._next_allowed - now
                    if wait_for <= 0:
                        self._next_allowed = now + self.interval
                        self._cv.notify_all()
                        return
                    self._cv.wait(timeout=wait_for)
            finally:
                if priority == self.REALTIME:
                    self._waiting_realtime -= 1
                    self._cv.notify_all()
