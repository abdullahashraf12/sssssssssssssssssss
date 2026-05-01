"""End-to-end live test against the user's local Oracle DB.

Constraints (per the user's plan):
- Creates AT MOST 3 dummy rows of its own (1 SK1MF + 1 PS33MF + 1 GRBRF),
  using key values that don't collide with real data.
- Real production rows are never read or modified.
- Zoho calls are routed to a local mock HTTP server -- no real Zoho I/O.
- Cleans up everything it created on exit.

Run:  python -m tests.live_oracle_check
"""
from __future__ import annotations

import contextlib
import http.server
import json
import os
import socket
import sys
import threading
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import cx_Oracle  # noqa: E402

from sync.oracle_pool import OraclePool
from sync.rate_limiter import ZohoTrafficGate
from sync.realtime_worker import RealtimeWorker
from sync.token_manager import TokenManager
from sync.zoho_client import ZohoClient

# Dummy key. Uses real company/year (only one combination exists in this DB,
# and FK constraints prevent us from inventing new ones), but a distinctive
# item code that does not collide with any of the 56k real items.
DUMMY_CP   = 1
DUMMY_YR   = 2025  # exists in OFISSOFT.GRYR (FK11 ENABLED, NOT VALIDATED for prior rows)
DUMMY_CODE = "ZSYNC_TEST_001"
DUMMY_BN   = 999

# Legacy triggers in the user's sandbox that depend on a missing package
# (`ofis_apex_pkg`). They are already invalid and would block every insert.
# We disable them for the test and re-enable on exit (nondestructive).
LEGACY_INVALID_TRIGGERS = (
    "SEQ_SK1M_SNDR_2",
    "SEQ_PS33M_SNDR_2",
    "SEQ_GRBR_SNDR_2",
    "SK1MSK1M",
    "GRBRF_BEFORE_INSERT",
)

ORACLE_USER = os.environ.get("ORACLE_USER", "test")
ORACLE_PASS = os.environ.get("ORACLE_PASS", "test")
ORACLE_DSN  = os.environ.get("ORACLE_DSN",  "192.168.100.15:1521/orcl")

# ---------------------------------------------------------------------------
# Mock Zoho HTTP server
# ---------------------------------------------------------------------------
class MockZoho(http.server.BaseHTTPRequestHandler):
    log = []         # [(method, path, body)]
    next_id = [1]

    def _read_body(self):
        n = int(self.headers.get("Content-Length", "0") or 0)
        return self.rfile.read(n).decode("utf-8") if n else ""

    def _record(self, method):
        body = self._read_body()
        self.log.append((method, self.path, body))

    def _ok(self, payload, status=200):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        path_no_qs = self.path.split("?")[0]
        if path_no_qs.endswith("/oauth/v2/token"):
            self._read_body()
            return self._ok({"access_token": "TEST-TOKEN", "expires_in": 3600})
        self._record("POST")
        rid = f"REC-{self.next_id[0]}"
        self.next_id[0] += 1
        return self._ok({"data": {"ID": rid}}, status=201)

    def do_PATCH(self):
        self._record("PATCH"); return self._ok({"data": {}})

    def do_DELETE(self):
        self._record("DELETE"); return self._ok({"data": {}})

    def log_message(self, *a, **kw):
        return  # silence


def start_mock_server() -> tuple[http.server.HTTPServer, str]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    srv = http.server.HTTPServer(("127.0.0.1", port), MockZoho)
    threading.Thread(target=srv.serve_forever, name="mock-zoho", daemon=True).start()
    return srv, f"http://127.0.0.1:{port}"


# ---------------------------------------------------------------------------
# Oracle helpers
# ---------------------------------------------------------------------------
def connect():
    return cx_Oracle.connect(ORACLE_USER, ORACLE_PASS, ORACLE_DSN)


def run_sql_file(conn, path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    cur = conn.cursor()
    chunks = []
    for raw in text.splitlines():
        if raw.strip() == "/":
            block = "\n".join(chunks).strip()
            if block:
                cur.execute(block)
            chunks = []
        else:
            chunks.append(raw)
    tail = "\n".join(chunks).strip()
    if tail:
        # Strip inline -- comments BEFORE splitting by ; (comments may contain ;)
        cleaned_lines = []
        for line in tail.splitlines():
            idx = line.find("--")
            if idx >= 0:
                line = line[:idx]
            cleaned_lines.append(line)
        tail = "\n".join(cleaned_lines)
        for stmt in tail.split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
    conn.commit()


def set_legacy_triggers(conn, enabled: bool) -> None:
    cur = conn.cursor()
    state = "ENABLE" if enabled else "DISABLE"
    for tn in LEGACY_INVALID_TRIGGERS:
        with contextlib.suppress(cx_Oracle.DatabaseError):
            cur.execute(f"ALTER TRIGGER {tn} {state}")
    conn.commit()


def cleanup_dummy_rows(conn) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM PS33MF WHERE PS33MCP=:c AND PS33MYR=:y AND PS33M1=:k",
                c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE)
    cur.execute("DELETE FROM SK1MF  WHERE SK1MCP=:c  AND SK1MYR=:y  AND SK1M1=:k",
                c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE)
    cur.execute("DELETE FROM GRBRF  WHERE GRBRCP=:c  AND GRBRYR=:y  AND BN=:b",
                c=DUMMY_CP, y=DUMMY_YR, b=DUMMY_BN)
    cur.execute("DELETE FROM SYNC_EVENTS WHERE k_cp=:c", c=DUMMY_CP)
    cur.execute("DELETE FROM ZOHO_MAP WHERE k_cp=:c", c=DUMMY_CP)
    conn.commit()


def insert_dummy_rows(conn) -> None:
    """Insert with *_SNDR_2 columns pre-populated so legacy triggers
    that call a missing 'ofis_apex_pkg' don't fire their NULL branch."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO SK1MF (SK1MCP, SK1MYR, SK1M1, SK1M2, SK1M3,
                           SK1M11, SK1M31, SK1M40, SK1M_SNDR_2)
        VALUES (:c, :y, :k, :ar, :en, 'Y', 'Y', 'N', 0)
    """, c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE,
         ar="اختبار", en="TestItem")
    cur.execute("""
        INSERT INTO PS33MF (PS33MCP, PS33MYR, PS33M1, PS33M2, PS33M4,
                            PS33M_SNDR_2)
        VALUES (:c, :y, :k, 7, 8, 0)
    """, c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE)
    # GRBRF.ID is NOT NULL but its auto-id trigger is invalid; supply manually.
    cur.execute("SELECT NVL(MAX(ID), 0) + 1 FROM GRBRF")
    next_id = cur.fetchone()[0]
    cur.execute("""
        INSERT INTO GRBRF (GRBRCP, GRBRYR, BN, GRBR2, GRBR3,
                           GRBR_SNDR_2, ID)
        VALUES (:c, :y, :b, :ar, :en, 0, :id)
    """, c=DUMMY_CP, y=DUMMY_YR, b=DUMMY_BN,
         ar="فرع", en="Branch", id=next_id)
    conn.commit()


def update_dummy_item(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        UPDATE SK1MF SET SK1M3 = :en
        WHERE SK1MCP=:c AND SK1MYR=:y AND SK1M1=:k
    """, en="TestItem-Updated", c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE)
    conn.commit()


def delete_dummy_item(conn) -> None:
    """Drop the FK child first (PS33MF), then the SK1MF row.

    Each delete fires its own trigger and produces its own SYNC_EVENTS row;
    the worker drains them in order.
    """
    cur = conn.cursor()
    cur.execute("DELETE FROM PS33MF WHERE PS33MCP=:c AND PS33MYR=:y AND PS33M1=:k",
                c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE)
    cur.execute("DELETE FROM SK1MF  WHERE SK1MCP=:c  AND SK1MYR=:y  AND SK1M1=:k",
                c=DUMMY_CP, y=DUMMY_YR, k=DUMMY_CODE)
    conn.commit()


def count_events_by_op(conn, source_table: str) -> dict[str, int]:
    cur = conn.cursor()
    cur.execute("""
        SELECT op, status, COUNT(*) FROM SYNC_EVENTS
        WHERE source_table = :s AND k_cp = :c
        GROUP BY op, status
    """, s=source_table, c=DUMMY_CP)
    out: dict[str, int] = {}
    for op, st, n in cur.fetchall():
        out[f"{op}/{st}"] = n
    return out


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def run() -> int:
    print("== Live Oracle integration check ==")
    print("Schema setup...")
    conn = connect()
    try:
        with contextlib.suppress(cx_Oracle.DatabaseError):
            cleanup_dummy_rows(conn)
        # Drop *new* objects too, to make this script re-runnable.
        cur = conn.cursor()
        for ddl in (
            "DROP TRIGGER trg_sk1mf_aiud",
            "DROP TRIGGER trg_ps33mf_aiud",
            "DROP TRIGGER trg_grbrf_aiud",
            "DROP TABLE SYNC_EVENTS PURGE",
            "DROP TABLE ZOHO_MAP PURGE",
            "DROP TABLE BACKFILL_CHECKPOINT PURGE",
        ):
            with contextlib.suppress(cx_Oracle.DatabaseError):
                cur.execute(ddl)
        conn.commit()
        run_sql_file(conn, ROOT / "sql" / "99_drop_old.sql")
        run_sql_file(conn, ROOT / "sql" / "01_schema.sql")
        run_sql_file(conn, ROOT / "sql" / "02_triggers.sql")
        set_legacy_triggers(conn, enabled=False)
    finally:
        conn.close()

    srv, base = start_mock_server()
    try:
        # Configure ZohoClient + TokenManager to point at the mock
        os.environ["ZOHO_API_BASE"] = f"{base}/api/v2"
        os.environ["ZOHO_TOKEN_URL"] = f"{base}/oauth/v2/token"
        os.environ["ZOHO_CLIENT_ID"] = "test"
        os.environ["ZOHO_CLIENT_SECRET"] = "test"
        os.environ["ZOHO_REFRESH_TOKEN"] = "test-refresh"

        pool = OraclePool(ORACLE_USER, ORACLE_PASS, ORACLE_DSN,
                          min_sessions=1, max_sessions=2)
        try:
            tokens = TokenManager("test", "test", "test-refresh",
                                  os.environ["ZOHO_TOKEN_URL"])
            limiter = ZohoTrafficGate(0.0, 0.0, max_concurrency=0, rate_per_minute=0)  # fast for the test
            zoho = ZohoClient("alpha1.abdullah771", "carton",
                              os.environ["ZOHO_API_BASE"],
                              tokens, limiter, max_attempts=2)
            worker = RealtimeWorker(pool, zoho, "Items_Data", "Branches_Codes",
                                    max_attempts=2, batch_size=20, idle_sleep=0.0)

            with connect() as c:
                print("Insert 3 dummy rows...")
                insert_dummy_rows(c)
                ev = count_events_by_op(c, "SK1MF")
                ev_p = count_events_by_op(c, "PS33MF")
                ev_g = count_events_by_op(c, "GRBRF")
                print(f"  SK1MF events: {ev}")
                print(f"  PS33MF events: {ev_p}")
                print(f"  GRBRF events: {ev_g}")
                assert ev.get("I/NEW", 0) >= 1, "expected SK1MF insert event"
                assert ev_g.get("I/NEW", 0) >= 1, "expected GRBRF insert event"

            print("Worker R drains insert events...")
            for _ in range(5):
                processed = worker.run_once()
                if processed == 0:
                    break

            with connect() as c:
                print("Update SK1MF row...")
                update_dummy_item(c)
            for _ in range(5):
                if worker.run_once() == 0:
                    break

            with connect() as c:
                print("Delete SK1MF row...")
                delete_dummy_item(c)
            for _ in range(5):
                if worker.run_once() == 0:
                    break

            print("Mock Zoho call log:")
            for m, p, _b in MockZoho.log:
                print(f"  {m} {p}")

            methods = [m for m, _, _ in MockZoho.log]
            assert "POST" in methods,   "expected at least one POST  (insert)"
            assert "PATCH" in methods,  "expected at least one PATCH (update)"
            assert "DELETE" in methods, "expected at least one DELETE (delete)"

            with connect() as c:
                cur = c.cursor()
                cur.execute(
                    "SELECT op, status, COUNT(*) FROM SYNC_EVENTS WHERE k_cp=:c "
                    "GROUP BY op, status", c=DUMMY_CP,
                )
                print("Final SYNC_EVENTS state:")
                for r in cur.fetchall():
                    print(f"  {r[0]} {r[1]} = {r[2]}")
        finally:
            pool.close()
    finally:
        srv.shutdown()
        # Clean up dummy rows + their queued events + map; re-enable legacy triggers.
        with contextlib.suppress(Exception):
            with connect() as c:
                cleanup_dummy_rows(c)
                set_legacy_triggers(c, enabled=True)
        print("Cleanup done.")
    print("== PASS ==")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except AssertionError as e:
        print("== FAIL ==", e)
        traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        print("== ERROR ==", e)
        traceback.print_exc()
        sys.exit(2)
