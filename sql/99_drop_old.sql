-- =====================================================================
-- Drop legacy objects superseded by the new sync architecture.
-- Idempotent: each drop is wrapped to ignore "does not exist" errors.
-- =====================================================================

DECLARE
    PROCEDURE drop_object(p_kind VARCHAR2, p_name VARCHAR2) IS
        v_sql VARCHAR2(200);
    BEGIN
        v_sql := 'DROP ' || p_kind || ' ' || p_name;
        IF p_kind = 'TABLE' THEN v_sql := v_sql || ' CASCADE CONSTRAINTS PURGE'; END IF;
        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE NOT IN (-942, -4080, -2289, -2443, -1418, -24010, -4043) THEN RAISE; END IF;
    END;
BEGIN
    drop_object('TRIGGER',   'trg_temp_skm_ai');
    drop_object('TRIGGER',   'trg_temp_grb_ai');
    drop_object('TRIGGER',   'trg_sk1mf_sync');
    drop_object('TRIGGER',   'trg_ps33m_sync');
    drop_object('TRIGGER',   'trg_grbrf_sync');
    drop_object('TRIGGER',   'trg_sk1mf_event');
    drop_object('PROCEDURE', 'fill_zho');
    drop_object('FUNCTION',  'zho_dequeue_event');
    drop_object('TABLE',     'TEMP_SKM');
    drop_object('TABLE',     'TEMP_GRB');
    drop_object('TABLE',     'ZOHO_SYNC_QUEUE');
END;
/

-- AQ queue tables (only if present) — tolerated to be missing.
BEGIN DBMS_AQADM.STOP_QUEUE(queue_name => 'ZHO_QUEUE');     EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN DBMS_AQADM.DROP_QUEUE(queue_name => 'ZHO_QUEUE');     EXCEPTION WHEN OTHERS THEN NULL; END;
/
BEGIN DBMS_AQADM.DROP_QUEUE_TABLE(queue_table => 'ZHO_QT'); EXCEPTION WHEN OTHERS THEN NULL; END;
/
COMMIT;
