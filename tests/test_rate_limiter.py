import threading
import time

from sync.rate_limiter import IntervalLimiter


class FakeClock:
    """Deterministic monotonic clock + sleep for test purposes."""
    def __init__(self):
        self.t = 0.0

    def time(self):
        return self.t

    def sleep(self, dt):
        if dt > 0:
            self.t += dt


def test_first_acquire_is_immediate():
    fc = FakeClock()
    lim = IntervalLimiter(2.0, time_func=fc.time, sleep_func=fc.sleep)
    lim.acquire()  # should not block / advance time
    assert fc.t == 0.0


def test_second_acquire_waits_full_interval():
    fc = FakeClock()
    lim = IntervalLimiter(2.0, time_func=fc.time, sleep_func=fc.sleep)
    lim.acquire()
    # We can't easily emulate Condition.wait timeout with FakeClock, so use real time:
    # switch to real-time test for spacing.
    real = IntervalLimiter(0.1)
    t0 = time.monotonic()
    real.acquire()
    real.acquire()
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.1


def test_realtime_priority_over_backfill():
    lim = IntervalLimiter(0.1)
    order: list[str] = []

    def backfill():
        lim.acquire(IntervalLimiter.BACKFILL)
        order.append("B1")
        lim.acquire(IntervalLimiter.BACKFILL)
        order.append("B2")

    def realtime():
        time.sleep(0.05)  # ensure backfill is already waiting on the gate
        lim.acquire(IntervalLimiter.REALTIME)
        order.append("R")

    tb = threading.Thread(target=backfill)
    tr = threading.Thread(target=realtime)
    tb.start(); tr.start()
    tb.join(timeout=2); tr.join(timeout=2)

    # First acquire is immediate (B1). Realtime should jump in front of B2.
    assert order[0] == "B1"
    assert "R" in order
    assert order.index("R") < order.index("B2")


def test_slow_down_doubles_interval_temporarily():
    lim = IntervalLimiter(0.05)
    lim.slow_down(factor=4.0, duration=10.0)
    assert lim.interval >= 0.2 - 1e-9
