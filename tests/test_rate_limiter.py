import threading
import time

from sync.rate_limiter import ZohoTrafficGate


def test_interval_zero_allows_immediate_sequential_calls():
    gate = ZohoTrafficGate(0.0, 1.0, max_concurrency=1, rate_per_minute=0)
    t0 = time.monotonic()
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    assert time.monotonic() - t0 < 0.05


def test_realtime_interval_spaces_only_realtime_calls():
    gate = ZohoTrafficGate(0.05, 0.0, max_concurrency=1, rate_per_minute=0)
    t0 = time.monotonic()
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    assert time.monotonic() - t0 >= 0.05

    t1 = time.monotonic()
    with gate.slot(ZohoTrafficGate.BACKFILL):
        pass
    assert time.monotonic() - t1 < 0.05


def test_backfill_interval_spaces_only_backfill_calls():
    gate = ZohoTrafficGate(0.0, 0.05, max_concurrency=1, rate_per_minute=0)
    t0 = time.monotonic()
    with gate.slot(ZohoTrafficGate.BACKFILL):
        pass
    with gate.slot(ZohoTrafficGate.BACKFILL):
        pass
    assert time.monotonic() - t0 >= 0.05

    t1 = time.monotonic()
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    assert time.monotonic() - t1 < 0.05


def test_token_bucket_waits_only_after_budget_is_empty():
    gate = ZohoTrafficGate(0.0, 0.0, max_concurrency=0, rate_per_minute=6000)
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    gate._tokens = 0.0  # force the next call to wait for a token refill
    t0 = time.monotonic()
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass
    assert time.monotonic() - t0 >= 0.005


def test_concurrency_cap_blocks_only_while_slot_is_active():
    gate = ZohoTrafficGate(0.0, 0.0, max_concurrency=1, rate_per_minute=0)
    entered = threading.Event()
    release = threading.Event()
    order: list[str] = []

    def first():
        with gate.slot(ZohoTrafficGate.REALTIME):
            order.append("first")
            entered.set()
            release.wait(timeout=2)

    def second():
        entered.wait(timeout=2)
        with gate.slot(ZohoTrafficGate.REALTIME):
            order.append("second")

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start(); t2.start()
    entered.wait(timeout=2)
    time.sleep(0.03)
    assert order == ["first"]
    release.set()
    t1.join(timeout=2); t2.join(timeout=2)
    assert order == ["first", "second"]


def _run_contested_order(
    gate: ZohoTrafficGate,
    first_waiter,
    second_waiter,
    holder_priority=ZohoTrafficGate.REALTIME,
):
    release = threading.Event()
    first_waiting = threading.Event()
    second_waiting = threading.Event()
    order: list[str] = []

    def holder():
        with gate.slot(holder_priority):
            order.append("holder")
            release.wait(timeout=2)

    def first():
        first_waiting.set()
        with gate.slot(first_waiter):
            order.append("first")

    def second():
        second_waiting.set()
        with gate.slot(second_waiter):
            order.append("second")

    threads = [threading.Thread(target=fn) for fn in (holder, first, second)]
    for t in threads:
        t.start()
    first_waiting.wait(timeout=2)
    second_waiting.wait(timeout=2)
    time.sleep(0.05)
    release.set()
    for t in threads:
        t.join(timeout=2)
    return order


def test_realtime_priority_when_intervals_are_equal():
    gate = ZohoTrafficGate(0.0, 0.0, max_concurrency=1, rate_per_minute=0)
    order = _run_contested_order(
        gate, ZohoTrafficGate.BACKFILL, ZohoTrafficGate.REALTIME,
    )
    assert order == ["holder", "second", "first"]


def test_realtime_priority_when_realtime_interval_is_smaller():
    gate = ZohoTrafficGate(0.0, 0.5, max_concurrency=1, rate_per_minute=0)
    order = _run_contested_order(
        gate, ZohoTrafficGate.BACKFILL, ZohoTrafficGate.REALTIME,
    )
    assert order == ["holder", "second", "first"]


def test_backfill_priority_when_backfill_interval_is_smaller():
    gate = ZohoTrafficGate(0.5, 0.0, max_concurrency=1, rate_per_minute=0)
    order = _run_contested_order(
        gate, ZohoTrafficGate.REALTIME, ZohoTrafficGate.BACKFILL,
        holder_priority=ZohoTrafficGate.BACKFILL,
    )
    assert order == ["holder", "second", "first"]


def test_ready_lower_priority_lane_runs_when_preferred_lane_is_not_ready():
    gate = ZohoTrafficGate(0.1, 0.0, max_concurrency=1, rate_per_minute=0)
    with gate.slot(ZohoTrafficGate.REALTIME):
        pass

    realtime_waiting = threading.Event()
    realtime_done = threading.Event()

    def realtime_waiter():
        realtime_waiting.set()
        with gate.slot(ZohoTrafficGate.REALTIME):
            realtime_done.set()

    t = threading.Thread(target=realtime_waiter)
    t.start()
    realtime_waiting.wait(timeout=2)
    time.sleep(0.02)

    t0 = time.monotonic()
    with gate.slot(ZohoTrafficGate.BACKFILL):
        pass
    assert time.monotonic() - t0 < 0.05

    t.join(timeout=2)
    assert realtime_done.is_set()
