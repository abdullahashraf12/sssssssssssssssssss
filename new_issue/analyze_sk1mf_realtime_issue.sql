-- =====================================================================
-- Diagnose why SK1MF-only INSERT rows do not appear in Zoho realtime sync.
--
-- Run from SQL*Plus:
--   sqlplus test/test@192.168.100.15:1521/orcl @new_issue/analyze_sk1mf_realtime_issue.sql
--
-- This script is intentionally idempotent for ITEM008-ITEM011:
--   * If a test SK1MF row does not exist, it inserts it.
--   * If it already exists, it prints that fact and leaves it unchanged.
--   * It does NOT insert PS33MF rows. The point is to prove whether the
--     current Worker R inner join can see these SK1MF-only rows.
-- =====================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET ECHO ON
SET FEEDBACK ON
SET PAGESIZE 200
SET LINESIZE 240
SET TRIMSPOOL ON
SET VERIFY OFF

COLUMN current_user_name FORMAT A20
COLUMN current_schema_name FORMAT A20
COLUMN table_name FORMAT A24
COLUMN trigger_name FORMAT A32
COLUMN status FORMAT A12
COLUMN triggering_event FORMAT A28
COLUMN code FORMAT A12
COLUMN diagnosis FORMAT A90
COLUMN expected_worker_behavior FORMAT A90
COLUMN last_error_short FORMAT A90
COLUMN zoho_record_id FORMAT A45
COLUMN source_table FORMAT A14
COLUMN op FORMAT A4

PROMPT
PROMPT =====================================================================
PROMPT 0) Session and object preflight
PROMPT =====================================================================

SELECT USER AS current_user_name,
       SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema_name
FROM dual;

SELECT owner, table_name
FROM all_tables
WHERE owner = 'TEST'
  AND table_name IN ('SK1MF', 'PS33MF', 'GRBRF', 'SYNC_EVENTS', 'ZOHO_MAP')
ORDER BY table_name;

SELECT owner, trigger_name, table_name, status, triggering_event
FROM all_triggers
WHERE owner = 'TEST'
  AND trigger_name IN ('TRG_SK1MF_AIUD', 'TRG_PS33MF_AIUD', 'TRG_GRBRF_AIUD')
ORDER BY trigger_name;

SELECT owner, name, type, line, position, text
FROM all_errors
WHERE owner = 'TEST'
  AND name IN ('TRG_SK1MF_AIUD', 'TRG_PS33MF_AIUD', 'TRG_GRBRF_AIUD')
ORDER BY name, sequence;

PROMPT
PROMPT =====================================================================
PROMPT 1) Capture current highest SYNC_EVENTS.ID before this test run
PROMPT =====================================================================

VAR before_max_id NUMBER

BEGIN
    SELECT NVL(MAX(id), 0)
      INTO :before_max_id
      FROM test.sync_events;

    DBMS_OUTPUT.PUT_LINE('[before] max SYNC_EVENTS.ID = ' || :before_max_id);
END;
/

PROMPT
PROMPT =====================================================================
PROMPT 2) State before inserting/checking ITEM008-ITEM011
PROMPT =====================================================================

WITH wanted (code) AS (
    SELECT 'ITEM008' FROM dual UNION ALL
    SELECT 'ITEM009' FROM dual UNION ALL
    SELECT 'ITEM010' FROM dual UNION ALL
    SELECT 'ITEM011' FROM dual
),
counts AS (
    SELECT w.code,
           (SELECT COUNT(*)
              FROM test.sk1mf s
             WHERE s.sk1mcp = 1
               AND s.sk1myr = 2024
               AND s.sk1m1 = w.code) AS sk1mf_count,
           (SELECT COUNT(*)
              FROM test.ps33mf p
             WHERE p.ps33mcp = 1
               AND p.ps33myr = 2024
               AND p.ps33m1 = w.code) AS ps33mf_count,
           (SELECT COUNT(*)
              FROM test.sk1mf s, test.ps33mf p
             WHERE p.ps33mcp = s.sk1mcp
               AND p.ps33myr = s.sk1myr
               AND p.ps33m1  = s.sk1m1
               AND s.sk1mcp = 1
               AND s.sk1myr = 2024
               AND s.sk1m1 = w.code) AS worker_join_count
    FROM wanted w
)
SELECT code, sk1mf_count, ps33mf_count, worker_join_count,
       CASE
           WHEN sk1mf_count = 0 THEN 'SK1MF row missing before test insert.'
           WHEN worker_join_count = 0 THEN 'SK1MF exists, but Worker R ITEMS_SELECT returns no row because PS33MF is missing.'
           ELSE 'Worker R can see a joined source row for this item.'
       END AS diagnosis
FROM counts
ORDER BY code;

PROMPT
PROMPT =====================================================================
PROMPT 3) Insert the four SK1MF test rows only if missing
PROMPT =====================================================================

DECLARE
    PROCEDURE ensure_sk1mf(
        p_code   IN VARCHAR2,
        p_ar     IN VARCHAR2,
        p_en     IN VARCHAR2,
        p_sender IN NUMBER
    ) IS
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*)
          INTO v_count
          FROM test.sk1mf
         WHERE sk1mcp = 1
           AND sk1myr = 2024
           AND sk1m1 = p_code;

        IF v_count = 0 THEN
            INSERT INTO test.sk1mf
                (sk1mcp, sk1myr, sk1m1, sk1m2, sk1m3, sk1m_sndr_2)
            VALUES
                (1, 2024, p_code, p_ar, p_en, p_sender);

            DBMS_OUTPUT.PUT_LINE('[inserted] TEST.SK1MF ' || p_code);
        ELSE
            DBMS_OUTPUT.PUT_LINE('[exists] TEST.SK1MF ' || p_code || ' already existed; not inserting again.');
        END IF;
    END;
BEGIN
    ensure_sk1mf('ITEM008', 'Test Item 8',  'Description for item 8',  1008);
    ensure_sk1mf('ITEM009', 'Test Item 9',  'Description for item 9',  1009);
    ensure_sk1mf('ITEM010', 'Test Item 10', 'Description for item 10', 1010);
    ensure_sk1mf('ITEM011', 'Test Item 11', 'Description for item 11', 1011);
END;
/

COMMIT;

PROMPT
PROMPT =====================================================================
PROMPT 4) Events generated after this script started
PROMPT =====================================================================

SELECT id, source_table, op, k_cp, k_yr, k_code, status, attempts,
       TO_CHAR(enqueued_at, 'YYYY-MM-DD HH24:MI:SS') AS enqueued_at,
       TO_CHAR(picked_at,   'YYYY-MM-DD HH24:MI:SS') AS picked_at,
       TO_CHAR(finished_at, 'YYYY-MM-DD HH24:MI:SS') AS finished_at,
       SUBSTR(last_error, 1, 90) AS last_error_short
FROM test.sync_events
WHERE id > :before_max_id
  AND k_cp = 1
  AND k_yr = 2024
  AND k_code IN ('ITEM008', 'ITEM009', 'ITEM010', 'ITEM011')
ORDER BY id;

PROMPT
PROMPT =====================================================================
PROMPT 5) All events for ITEM008-ITEM011, including older/rerun events
PROMPT =====================================================================

SELECT id, source_table, op, k_cp, k_yr, k_code, status, attempts,
       TO_CHAR(enqueued_at, 'YYYY-MM-DD HH24:MI:SS') AS enqueued_at,
       TO_CHAR(picked_at,   'YYYY-MM-DD HH24:MI:SS') AS picked_at,
       TO_CHAR(finished_at, 'YYYY-MM-DD HH24:MI:SS') AS finished_at,
       SUBSTR(last_error, 1, 90) AS last_error_short
FROM test.sync_events
WHERE k_cp = 1
  AND k_yr = 2024
  AND k_code IN ('ITEM008', 'ITEM009', 'ITEM010', 'ITEM011')
ORDER BY id;

PROMPT
PROMPT =====================================================================
PROMPT 6) Exact source availability from the worker's point of view
PROMPT =====================================================================

WITH wanted (code) AS (
    SELECT 'ITEM008' FROM dual UNION ALL
    SELECT 'ITEM009' FROM dual UNION ALL
    SELECT 'ITEM010' FROM dual UNION ALL
    SELECT 'ITEM011' FROM dual
),
counts AS (
    SELECT w.code,
           (SELECT COUNT(*)
              FROM test.sk1mf s
             WHERE s.sk1mcp = 1
               AND s.sk1myr = 2024
               AND s.sk1m1 = w.code) AS sk1mf_count,
           (SELECT COUNT(*)
              FROM test.ps33mf p
             WHERE p.ps33mcp = 1
               AND p.ps33myr = 2024
               AND p.ps33m1 = w.code) AS ps33mf_count,
           (SELECT COUNT(*)
              FROM test.sk1mf s, test.ps33mf p
             WHERE p.ps33mcp = s.sk1mcp
               AND p.ps33myr = s.sk1myr
               AND p.ps33m1  = s.sk1m1
               AND s.sk1mcp = 1
               AND s.sk1myr = 2024
               AND s.sk1m1 = w.code) AS worker_join_count
    FROM wanted w
)
SELECT code, sk1mf_count, ps33mf_count, worker_join_count,
       CASE
           WHEN sk1mf_count = 1 AND ps33mf_count = 0 THEN
               'EXPECTED NO ZOHO CALL: Worker R selects SK1MF,PS33MF with an inner join and PS33MF is missing.'
           WHEN worker_join_count = 1 THEN
               'EXPECTED ZOHO ADD/UPDATE: Worker R can build Items_Data payload from SK1MF + PS33MF.'
           WHEN sk1mf_count = 0 THEN
               'NO SOURCE ROW: SK1MF row is missing.'
           ELSE
               'CHECK DUPLICATES OR KEY DATA: counts are not the normal 0/1 case.'
       END AS expected_worker_behavior
FROM counts
ORDER BY code;

PROMPT
PROMPT The next SELECT is the same inner-join shape used by sync/realtime_worker.py.
PROMPT If it returns no rows for these items, Worker R has no item payload to send.

SELECT s.sk1mcp, s.sk1myr, s.sk1m1, s.sk1m2, s.sk1m3,
       p.ps33m2, p.ps33m4
FROM test.sk1mf s, test.ps33mf p
WHERE p.ps33mcp = s.sk1mcp
  AND p.ps33myr = s.sk1myr
  AND p.ps33m1  = s.sk1m1
  AND s.sk1mcp = 1
  AND s.sk1myr = 2024
  AND s.sk1m1 IN ('ITEM008', 'ITEM009', 'ITEM010', 'ITEM011')
ORDER BY s.sk1m1;

PROMPT
PROMPT =====================================================================
PROMPT 7) Zoho map state for these keys
PROMPT =====================================================================

SELECT source_table, k_cp, k_yr, k_code, zoho_record_id,
       TO_CHAR(last_synced_at, 'YYYY-MM-DD HH24:MI:SS') AS last_synced_at
FROM test.zoho_map
WHERE source_table IN ('ITEMS', 'SK1MF')
  AND k_cp = 1
  AND k_yr = 2024
  AND k_code IN ('ITEM008', 'ITEM009', 'ITEM010', 'ITEM011')
ORDER BY source_table, k_code;

PROMPT
PROMPT =====================================================================
PROMPT 8) Per-item conclusion prints
PROMPT =====================================================================

DECLARE
    TYPE code_tab IS TABLE OF VARCHAR2(20);
    v_codes code_tab := code_tab('ITEM008', 'ITEM009', 'ITEM010', 'ITEM011');
    v_sk1mf_count NUMBER;
    v_ps33mf_count NUMBER;
    v_join_count NUMBER;
    v_new_events NUMBER;
    v_done_events NUMBER;
    v_dead_events NUMBER;
    v_map_count NUMBER;
BEGIN
    FOR i IN 1 .. v_codes.COUNT LOOP
        SELECT COUNT(*)
          INTO v_sk1mf_count
          FROM test.sk1mf
         WHERE sk1mcp = 1
           AND sk1myr = 2024
           AND sk1m1 = v_codes(i);

        SELECT COUNT(*)
          INTO v_ps33mf_count
          FROM test.ps33mf
         WHERE ps33mcp = 1
           AND ps33myr = 2024
           AND ps33m1 = v_codes(i);

        SELECT COUNT(*)
          INTO v_join_count
          FROM test.sk1mf s, test.ps33mf p
         WHERE p.ps33mcp = s.sk1mcp
           AND p.ps33myr = s.sk1myr
           AND p.ps33m1  = s.sk1m1
           AND s.sk1mcp = 1
           AND s.sk1myr = 2024
           AND s.sk1m1 = v_codes(i);

        SELECT COUNT(CASE WHEN status = 'NEW' THEN 1 END),
               COUNT(CASE WHEN status = 'DONE' THEN 1 END),
               COUNT(CASE WHEN status = 'DEAD' THEN 1 END)
          INTO v_new_events, v_done_events, v_dead_events
          FROM test.sync_events
         WHERE k_cp = 1
           AND k_yr = 2024
           AND k_code = v_codes(i);

        SELECT COUNT(*)
          INTO v_map_count
          FROM test.zoho_map
         WHERE source_table = 'ITEMS'
           AND k_cp = 1
           AND k_yr = 2024
           AND k_code = v_codes(i);

        DBMS_OUTPUT.PUT_LINE(
            '[diag] ' || v_codes(i) ||
            ' SK1MF=' || v_sk1mf_count ||
            ' PS33MF=' || v_ps33mf_count ||
            ' WORKER_JOIN=' || v_join_count ||
            ' EVENTS(NEW/DONE/DEAD)=' || v_new_events || '/' || v_done_events || '/' || v_dead_events ||
            ' ZOHO_MAP=' || v_map_count
        );

        IF v_sk1mf_count > 0 AND v_join_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE(
                '[root-cause-candidate] ' || v_codes(i) ||
                ': SK1MF exists but the Worker R item SELECT returns no row. A matching TEST.PS33MF row is required for Zoho Items_Data.'
            );
        ELSIF v_join_count > 0 AND v_map_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE(
                '[next-check] ' || v_codes(i) ||
                ': joined source row exists but no ZOHO_MAP yet. Check SYNC_EVENTS status and last_error.'
            );
        ELSIF v_map_count > 0 THEN
            DBMS_OUTPUT.PUT_LINE(
                '[ok] ' || v_codes(i) ||
                ': ZOHO_MAP contains an ITEMS record id, so Zoho add/update likely succeeded.'
            );
        END IF;
    END LOOP;
END;
/

PROMPT
PROMPT =====================================================================
PROMPT 9) What to look for
PROMPT =====================================================================
PROMPT - If WORKER_JOIN_COUNT = 0, realtime_worker.py will not call Zoho for that item.
PROMPT - If events are DONE but ZOHO_MAP is empty and WORKER_JOIN_COUNT = 0, the event was a no-op caused by missing PS33MF.
PROMPT - If events are NEW, the worker did not pick them up yet or is connected to another schema/database.
PROMPT - If events are DEAD, inspect LAST_ERROR above.
PROMPT - Rerun this same script after a few seconds while Worker R is running; it will skip duplicate SK1MF inserts and reprint status.

