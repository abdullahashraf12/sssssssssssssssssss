-- =====================================================================
-- Migration: add delayed retry support for realtime SYNC_EVENTS.
--
-- Run as schema owner after 01_schema.sql has already been deployed:
--   sqlplus test/test@192.168.100.15:1521/orcl @sql/03_add_next_attempt_at.sql
-- =====================================================================

BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE SYNC_EVENTS ADD (next_attempt_at TIMESTAMP)';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -1430 THEN -- ORA-01430: column being added already exists
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP INDEX ix_sync_events_status';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -1418 THEN -- ORA-01418: specified index does not exist
            RAISE;
        END IF;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'CREATE INDEX ix_sync_events_ready ON SYNC_EVENTS(status, next_attempt_at, id)';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN -- ORA-00955: name is already used by an existing object
            RAISE;
        END IF;
END;
/

COMMIT;
