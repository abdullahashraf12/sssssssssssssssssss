"""Worker R: drains SYNC_EVENTS into Zoho. Realtime priority on the limiter."""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from . import zoho_map
from .rate_limiter import IntervalLimiter
from .transform import branches_payload, items_payload
from .zoho_client import ZohoClient, ZohoError

log = logging.getLogger(__name__)

ITEMS_SELECT = """
SELECT s.SK1MCP, s.SK1MYR, s.SK1M1, s.SK1M2, s.SK1M3, s.SK1M9,
       s.SK1M11, s.SK1M12, s.SK1M13, s.SK1M14, s.SK1M16, s.SK1M17,
       s.SK1M18, s.SK1M19, s.SK1M20, s.SK1M21, s.SK1M22, s.SK1M24,
       s.SK1M29, s.SK1M31, s.SK1M32, s.SK1M33, s.SK1M34, s.SK1M36,
       s.SK1M37, s.SK1M39, s.SK1M40, s.SK1M41, s.SK1M261,
       p.PS33M2, p.PS33M4
FROM   sk1mf s, ps33mf p
WHERE  ps33mcp = sk1mcp
  AND  ps33myr = sk1myr
  AND  ps33m1  = sk1m1
  AND  s.SK1MCP = :cp AND s.SK1MYR = :yr AND s.SK1M1 = :code
"""

BRANCHES_SELECT = """
SELECT GRBRCP, GRBRYR, BN, GRBR2, GRBR3
FROM   GRBRF
WHERE  GRBRCP = :cp AND GRBRYR = :yr AND BN = :bn
"""


def _row_to_dict(cursor, row) -> dict[str, Any]:
    cols = [c[0].upper() for c in cursor.description]
    return dict(zip(cols, row))


class RealtimeWorker:
    def __init__(self, pool, zoho: ZohoClient, form_items: str, form_branches: str,
                 max_attempts: int = 5, batch_size: int = 10, idle_sleep: float = 1.0,
                 stop_event: threading.Event | None = None):
        self._pool = pool
        self._zoho = zoho
        self._form_items = form_items
        self._form_branches = form_branches
        self._max_attempts = max_attempts
        self._batch_size = batch_size
        self._idle_sleep = idle_sleep
        self._stop = stop_event or threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        log.info("realtime worker started")
        while not self._stop.is_set():
            processed = self.run_once()
            if processed == 0:
                if self._stop.wait(self._idle_sleep):
                    break
        log.info("realtime worker stopped")

    def run_once(self) -> int:
        with self._pool.connection() as conn:
            cursor = conn.cursor()
            events = self._pick_events(cursor)
            if not events:
                conn.commit()
                return 0
            for ev in events:
                self._handle_event(conn, ev)
            conn.commit()
            return len(events)

    def _pick_events(self, cursor) -> list[dict[str, Any]]:
        """Two-phase claim that works on Oracle 12.2 (no FOR UPDATE...FETCH FIRST):

          1. Read up to N candidate ids (no row lock).
          2. Claim each via UPDATE ... WHERE id=:id AND status='NEW'.
             Only one worker can win the race.
        """
        cursor.execute(
            """
            SELECT id, source_table, op, k_cp, k_yr, k_code, k_bn, attempts
            FROM SYNC_EVENTS
            WHERE status = 'NEW'
            ORDER BY id
            FETCH FIRST :n ROWS ONLY
            """,
            n=self._batch_size,
        )
        cols = [c[0].lower() for c in cursor.description]
        candidates = [dict(zip(cols, r)) for r in cursor.fetchall()]
        if not candidates:
            return []
        claimed: list[dict[str, Any]] = []
        for ev in candidates:
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status='INFLIGHT', picked_at=SYSTIMESTAMP "
                "WHERE id=:id AND status='NEW'",
                id=ev["id"],
            )
            if cursor.rowcount == 1:
                claimed.append(ev)
        return claimed

    def _handle_event(self, conn, ev: dict[str, Any]) -> None:
        cursor = conn.cursor()
        event_id = ev["id"]
        try:
            if ev["source_table"] == "GRBRF":
                self._sync_branches(cursor, ev)
            else:  # SK1MF or PS33MF -> sync the corresponding Items_Data row
                self._sync_items(cursor, ev)
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status='DONE', finished_at=SYSTIMESTAMP, "
                "last_error=NULL WHERE id=:id",
                id=event_id,
            )
        except Exception as e:
            log.exception("event %s failed", event_id)
            new_attempts = ev["attempts"] + 1
            new_status = "DEAD" if new_attempts >= self._max_attempts else "NEW"
            cursor.execute(
                "UPDATE SYNC_EVENTS SET status=:st, attempts=:a, last_error=:err "
                "WHERE id=:id",
                st=new_status, a=new_attempts, err=str(e)[:3900], id=event_id,
            )

    # --- per-form handlers
    def _sync_items(self, cursor, ev: dict[str, Any]) -> None:
        cp, yr, code = ev["k_cp"], ev["k_yr"], ev["k_code"]
        cursor.execute(ITEMS_SELECT, cp=cp, yr=yr, code=code)
        row = cursor.fetchone()
        existing_id = zoho_map.lookup(cursor, "ITEMS",
                                      k_cp=cp, k_yr=yr, k_code=code)

        if row is None:
            # Joined view does not produce a row -> mirror as DELETE to Zoho.
            if existing_id:
                self._zoho.delete_record(self._form_items, existing_id,
                                         priority=IntervalLimiter.REALTIME)
                zoho_map.delete(cursor, "ITEMS", k_cp=cp, k_yr=yr, k_code=code)
            return

        payload = items_payload(_row_to_dict(cursor, row))
        if ev["op"] == "D":
            if existing_id:
                self._zoho.delete_record(self._form_items, existing_id,
                                         priority=IntervalLimiter.REALTIME)
                zoho_map.delete(cursor, "ITEMS", k_cp=cp, k_yr=yr, k_code=code)
            return

        if existing_id:
            try:
                self._zoho.update_record(self._form_items, existing_id, payload,
                                         priority=IntervalLimiter.REALTIME)
                return
            except ZohoError as e:
                if e.status_code != 404:
                    raise
                zoho_map.delete(cursor, "ITEMS", k_cp=cp, k_yr=yr, k_code=code)

        new_id = self._zoho.add_record(self._form_items, payload,
                                       priority=IntervalLimiter.REALTIME)
        zoho_map.upsert(cursor, "ITEMS", new_id,
                        k_cp=cp, k_yr=yr, k_code=code)

    def _sync_branches(self, cursor, ev: dict[str, Any]) -> None:
        cp, yr, bn = ev["k_cp"], ev["k_yr"], ev["k_bn"]
        existing_id = zoho_map.lookup(cursor, "GRBRF",
                                      k_cp=cp, k_yr=yr, k_bn=bn)
        if ev["op"] == "D":
            if existing_id:
                self._zoho.delete_record(self._form_branches, existing_id,
                                         priority=IntervalLimiter.REALTIME)
                zoho_map.delete(cursor, "GRBRF", k_cp=cp, k_yr=yr, k_bn=bn)
            return

        cursor.execute(BRANCHES_SELECT, cp=cp, yr=yr, bn=bn)
        row = cursor.fetchone()
        if row is None:
            if existing_id:
                self._zoho.delete_record(self._form_branches, existing_id,
                                         priority=IntervalLimiter.REALTIME)
                zoho_map.delete(cursor, "GRBRF", k_cp=cp, k_yr=yr, k_bn=bn)
            return

        payload = branches_payload(_row_to_dict(cursor, row))
        if existing_id:
            try:
                self._zoho.update_record(self._form_branches, existing_id, payload,
                                         priority=IntervalLimiter.REALTIME)
                return
            except ZohoError as e:
                if e.status_code != 404:
                    raise
                zoho_map.delete(cursor, "GRBRF", k_cp=cp, k_yr=yr, k_bn=bn)

        new_id = self._zoho.add_record(self._form_branches, payload,
                                       priority=IntervalLimiter.REALTIME)
        zoho_map.upsert(cursor, "GRBRF", new_id,
                        k_cp=cp, k_yr=yr, k_bn=bn)
