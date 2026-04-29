from sync import zoho_map
from .fakes import FakeConnection


def test_lookup_returns_none_when_no_row():
    conn = FakeConnection()
    conn.register("FROM ZOHO_MAP", lambda *_: ([], []))
    cur = conn.cursor()
    assert zoho_map.lookup(cur, "ITEMS", k_cp=1, k_yr=2025, k_code="X") is None


def test_lookup_returns_id_when_row_present():
    conn = FakeConnection()
    conn.register("FROM ZOHO_MAP",
                  lambda *_: (["zoho_record_id"], [("REC-3",)]))
    cur = conn.cursor()
    assert zoho_map.lookup(cur, "ITEMS", k_cp=1, k_yr=2025, k_code="X") == "REC-3"


def test_upsert_runs_merge():
    conn = FakeConnection()
    conn.register("MERGE INTO ZOHO_MAP", lambda *_: None)
    cur = conn.cursor()
    zoho_map.upsert(cur, "ITEMS", "REC-9", k_cp=1, k_yr=2025, k_code="X")
    assert any("MERGE INTO ZOHO_MAP" in s for s, _ in conn.execute_calls)


def test_delete_runs_delete():
    conn = FakeConnection()
    conn.register("DELETE FROM ZOHO_MAP", lambda *_: None)
    cur = conn.cursor()
    zoho_map.delete(cur, "ITEMS", k_cp=1, k_yr=2025, k_code="X")
    assert any("DELETE FROM ZOHO_MAP" in s for s, _ in conn.execute_calls)
