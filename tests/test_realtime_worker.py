from sync.realtime_worker import RealtimeWorker

from .fakes import FakeConnection, FakePool, FakeZoho


ITEMS_COLS = (
    "SK1MCP SK1MYR SK1M1 SK1M2 SK1M3 SK1M9 SK1M11 SK1M12 SK1M13 SK1M14 "
    "SK1M16 SK1M17 SK1M18 SK1M19 SK1M20 SK1M21 SK1M22 SK1M24 SK1M29 "
    "SK1M31 SK1M32 SK1M33 SK1M34 SK1M36 SK1M37 SK1M39 SK1M40 SK1M41 "
    "SK1M261 PS33M2 PS33M4"
).split()
SAMPLE_ITEMS_ROW = (
    1, 2025, "ITM-1", "اسم", "Name", 1,
    "Y", "N", "Y", 14, 16, 17, 18, 19, 20, 21, 22,
    "AC", "PAR", "Y", "N", "Y", 34, "KG", 37, 39, "N", "Y",
    261, 5, 6,
)


def _new_event(eid=1, op="I", source="SK1MF", cp=1, yr=2025, code="ITM-1", bn=None,
               attempts=0):
    return (eid, source, op, cp, yr, code, bn, attempts)


def _events_handler(events):
    cols = ["id", "source_table", "op", "k_cp", "k_yr", "k_code", "k_bn", "attempts"]
    def h(_sql, _p):
        return cols, list(events)
    return h


def _empty_handler():
    cols = ["id", "source_table", "op", "k_cp", "k_yr", "k_code", "k_bn", "attempts"]
    def h(_sql, _p):
        return cols, []
    return h


def _make_worker(conn=None, zoho=None):
    pool = FakePool(conn or FakeConnection())
    zoho = zoho or FakeZoho()
    return pool, zoho, RealtimeWorker(
        pool, zoho, "Items_Data", "Branches_Codes",
        max_attempts=3, batch_size=10, idle_sleep=0.0,
    )


def test_run_once_returns_zero_when_no_events():
    conn = FakeConnection()
    conn.register("SYNC_EVENTS", _empty_handler())
    pool, _, w = _make_worker(conn=conn)
    assert w.run_once() == 0
    assert conn.commit_count == 1


def test_insert_event_calls_add_and_upserts_map():
    conn = FakeConnection()
    conn.register("SYNC_EVENTS", _events_handler([_new_event()]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)         # mark INFLIGHT
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, [SAMPLE_ITEMS_ROW]))
    conn.register("FROM ZOHO_MAP", lambda *_: ([], []))           # not present
    conn.register("MERGE INTO ZOHO_MAP", lambda *_: None)
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)         # mark DONE

    pool, zoho, w = _make_worker(conn=conn)
    assert w.run_once() == 1
    kinds = [c[0] for c in zoho.calls]
    assert kinds == ["add"]


def test_update_event_uses_existing_zoho_id():
    conn = FakeConnection()
    conn.register("SYNC_EVENTS", _events_handler([_new_event(op="U")]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, [SAMPLE_ITEMS_ROW]))
    conn.register("FROM ZOHO_MAP", lambda *_: (["zoho_record_id"], [("REC-99",)]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)

    pool, zoho, w = _make_worker(conn=conn)
    w.run_once()
    assert zoho.calls[0][0] == "update"
    assert zoho.calls[0][2] == "REC-99"


def test_delete_event_calls_delete_when_mapped():
    conn = FakeConnection()
    conn.register("SYNC_EVENTS", _events_handler([_new_event(op="D")]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, [SAMPLE_ITEMS_ROW]))
    conn.register("FROM ZOHO_MAP", lambda *_: (["zoho_record_id"], [("REC-50",)]))
    conn.register("DELETE FROM ZOHO_MAP", lambda *_: None)
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)

    pool, zoho, w = _make_worker(conn=conn)
    w.run_once()
    assert zoho.calls == [("delete", "Items_Data", "REC-50", 0)]


def test_ps33mf_delete_breaking_join_propagates_delete_to_zoho():
    """User deletes a PS33MF row -> joined view stops producing the SKM row.
    If a Zoho record exists for that key, mirror the delete."""
    conn = FakeConnection()
    conn.register("SYNC_EVENTS",
                  _events_handler([_new_event(op="D", source="PS33MF")]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, []))           # no row
    conn.register("FROM ZOHO_MAP", lambda *_: (["zoho_record_id"], [("REC-7",)]))
    conn.register("DELETE FROM ZOHO_MAP", lambda *_: None)
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)

    pool, zoho, w = _make_worker(conn=conn)
    w.run_once()
    assert zoho.calls == [("delete", "Items_Data", "REC-7", 0)]


def test_ps33mf_event_with_no_zoho_record_is_noop():
    conn = FakeConnection()
    conn.register("SYNC_EVENTS",
                  _events_handler([_new_event(op="D", source="PS33MF")]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, []))
    conn.register("FROM ZOHO_MAP", lambda *_: ([], []))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)

    pool, zoho, w = _make_worker(conn=conn)
    w.run_once()
    assert zoho.calls == []


def test_branches_insert_path():
    conn = FakeConnection()
    conn.register("SYNC_EVENTS", _events_handler([
        (1, "GRBRF", "I", 1, 2025, None, 7, 0)
    ]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)
    conn.register("FROM ZOHO_MAP", lambda *_: ([], []))
    conn.register("FROM   GRBRF", lambda *_: (
        ["GRBRCP", "GRBRYR", "BN", "GRBR2", "GRBR3"],
        [(1, 2025, 7, "ع", "E")],
    ))
    conn.register("MERGE INTO ZOHO_MAP", lambda *_: None)
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)

    pool, zoho, w = _make_worker(conn=conn)
    w.run_once()
    assert zoho.calls[0][:2] == ("add", "Branches_Codes")


def test_failure_increments_attempts_and_marks_dead_after_max():
    class BoomZoho(FakeZoho):
        def add_record(self, *a, **kw):
            raise RuntimeError("boom")

    conn = FakeConnection()
    # attempts already at max-1=2, so next failure -> DEAD
    conn.register("SYNC_EVENTS",
                  _events_handler([_new_event(attempts=2)]))
    conn.register("UPDATE SYNC_EVENTS", lambda *_: None)
    conn.register("FROM   sk1mf", lambda *_: (ITEMS_COLS, [SAMPLE_ITEMS_ROW]))
    conn.register("FROM ZOHO_MAP", lambda *_: ([], []))
    captured: dict = {}

    def fail_update(_sql, params):
        captured.update(params)

    conn.register("UPDATE SYNC_EVENTS SET status=:st", fail_update)

    pool, _, w = _make_worker(conn=conn, zoho=BoomZoho())
    w.run_once()
    assert captured.get("st") == "DEAD"
    assert captured.get("a") == 3
