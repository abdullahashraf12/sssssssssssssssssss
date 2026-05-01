"""Global Zoho traffic gate with separate realtime/backfill intervals."""
from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Iterator


class ZohoTrafficGate:
    REALTIME = 0
    BACKFILL = 1

    def __init__(
        self,
        realtime_interval_seconds: float = 0.0,
        backfill_interval_seconds: float = 0.0,
        *,
        max_concurrency: int = 6,
        rate_per_minute: float = 50.0,
        time_func=time.monotonic,
    ):
        if realtime_interval_seconds < 0:
            raise ValueError("realtime_interval_seconds must be >= 0")
        if backfill_interval_seconds < 0:
            raise ValueError("backfill_interval_seconds must be >= 0")
        if max_concurrency < 0:
            raise ValueError("max_concurrency must be >= 0")
        if rate_per_minute < 0:
            raise ValueError("rate_per_minute must be >= 0")
        self._intervals = {
            self.REALTIME: float(realtime_interval_seconds),
            self.BACKFILL: float(backfill_interval_seconds),
        }
        self._max_concurrency = int(max_concurrency)
        self._rate_per_minute = float(rate_per_minute)
        self._time = time_func
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._next_allowed = {self.REALTIME: 0.0, self.BACKFILL: 0.0}
        self._active = 0
        self._waiting = {self.REALTIME: 0, self.BACKFILL: 0}
        self._slowdown_until = 0.0
        self._slowdown_factor = 1.0
        self._tokens = self._bucket_capacity()
        self._last_refill = self._time()

    @property
    def interval(self) -> float:
        return self.interval_for(self.REALTIME)

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    def interval_for(self, priority: int) -> float:
        return self._intervals[self._normalize_priority(priority)] * self._effective_factor()

    def _effective_factor(self) -> float:
        if self._time() < self._slowdown_until:
            return self._slowdown_factor
        return 1.0

    def _effective_rate_per_minute(self) -> float:
        if self._rate_per_minute <= 0:
            return 0.0
        return self._rate_per_minute / self._effective_factor()

    def _bucket_capacity(self) -> float:
        return self._rate_per_minute if self._rate_per_minute > 0 else 0.0

    def _refill_tokens(self, now: float) -> None:
        if self._rate_per_minute <= 0:
            return
        elapsed = max(0.0, now - self._last_refill)
        self._last_refill = now
        refill_per_second = self._effective_rate_per_minute() / 60.0
        self._tokens = min(self._bucket_capacity(), self._tokens + elapsed * refill_per_second)

    def slow_down(self, factor: float = 2.0, duration: float = 60.0) -> None:
        if factor <= 1.0 or duration <= 0:
            return
        with self._cv:
            self._slowdown_factor = max(self._slowdown_factor, factor)
            self._slowdown_until = max(self._slowdown_until, self._time() + duration)
            self._cv.notify_all()

    @contextlib.contextmanager
    def slot(self, priority: int = REALTIME) -> Iterator[None]:
        self.acquire(priority=priority)
        try:
            yield
        finally:
            self.release()

    def acquire(self, priority: int = REALTIME) -> None:
        """Block until a Zoho API call may start.

        The returned permission owns one concurrency slot.  Call ``release()``
        after the HTTP request finishes, or use ``slot()`` as a context manager.
        """
        priority = self._normalize_priority(priority)
        with self._cv:
            self._waiting[priority] += 1
            try:
                while True:
                    now = self._time()
                    self._refill_tokens(now)

                    if self._max_concurrency and self._active >= self._max_concurrency:
                        self._cv.wait(timeout=0.02)
                        continue

                    wait_for = self._next_allowed[priority] - now
                    if wait_for > 0:
                        self._cv.wait(timeout=min(wait_for, 0.1))
                        continue

                    if self._rate_per_minute > 0 and self._tokens < 1.0:
                        refill = max(self._effective_rate_per_minute() / 60.0, 0.001)
                        self._cv.wait(timeout=min((1.0 - self._tokens) / refill, 0.1))
                        continue

                    if self._should_yield_to_other_lane(priority, now):
                        self._cv.wait(timeout=0.02)
                        continue

                    interval = self.interval_for(priority)
                    if interval > 0:
                        self._next_allowed[priority] = now + interval
                    if self._rate_per_minute > 0:
                        self._tokens -= 1.0
                    self._active += 1
                    return
            finally:
                self._waiting[priority] -= 1
                self._cv.notify_all()

    def release(self) -> None:
        with self._cv:
            if self._active > 0:
                self._active -= 1
            self._cv.notify_all()

    def _normalize_priority(self, priority: int) -> int:
        if priority not in self._intervals:
            raise ValueError(f"unknown Zoho traffic priority: {priority!r}")
        return priority

    def _preferred_lane(self) -> int:
        realtime = self._intervals[self.REALTIME]
        backfill = self._intervals[self.BACKFILL]
        return self.REALTIME if realtime <= backfill else self.BACKFILL

    def _should_yield_to_other_lane(self, priority: int, now: float) -> bool:
        other = self.BACKFILL if priority == self.REALTIME else self.REALTIME
        if self._waiting[other] <= 0:
            return False
        if self._next_allowed[other] > now:
            return False
        if self._preferred_lane() != other:
            return False

        concurrency_contested = (
            self._max_concurrency > 0
            and self._active >= self._max_concurrency - 1
        )
        token_contested = self._rate_per_minute > 0 and self._tokens < 2.0
        return concurrency_contested or token_contested


# Backward-compatible name used by existing workers/tests.
IntervalLimiter = ZohoTrafficGate
