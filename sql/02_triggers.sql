-- =====================================================================
-- Source-table triggers: every user-driven I/U/D becomes a SYNC_EVENTS row.
-- One row per change. Worker R picks them up with FOR UPDATE SKIP LOCKED.
-- =====================================================================

CREATE OR REPLACE TRIGGER trg_sk1mf_aiud
AFTER INSERT OR UPDATE OR DELETE ON SK1MF
FOR EACH ROW
DECLARE
    v_op   VARCHAR2(1);
    v_cp   NUMBER;
    v_yr   NUMBER;
    v_code VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_op := 'I'; v_cp := :NEW.SK1MCP; v_yr := :NEW.SK1MYR; v_code := :NEW.SK1M1;
    ELSIF UPDATING THEN
        v_op := 'U'; v_cp := :NEW.SK1MCP; v_yr := :NEW.SK1MYR; v_code := :NEW.SK1M1;
    ELSE
        v_op := 'D'; v_cp := :OLD.SK1MCP; v_yr := :OLD.SK1MYR; v_code := :OLD.SK1M1;
    END IF;

    INSERT INTO SYNC_EVENTS (source_table, op, k_cp, k_yr, k_code)
    VALUES ('SK1MF', v_op, v_cp, v_yr, v_code);
END;
/

CREATE OR REPLACE TRIGGER trg_ps33mf_aiud
AFTER INSERT OR UPDATE OR DELETE ON PS33MF
FOR EACH ROW
DECLARE
    v_op   VARCHAR2(1);
    v_cp   NUMBER;
    v_yr   NUMBER;
    v_code VARCHAR2(20);
BEGIN
    IF INSERTING THEN
        v_op := 'I'; v_cp := :NEW.PS33MCP; v_yr := :NEW.PS33MYR; v_code := :NEW.PS33M1;
    ELSIF UPDATING THEN
        v_op := 'U'; v_cp := :NEW.PS33MCP; v_yr := :NEW.PS33MYR; v_code := :NEW.PS33M1;
    ELSE
        v_op := 'D'; v_cp := :OLD.PS33MCP; v_yr := :OLD.PS33MYR; v_code := :OLD.PS33M1;
    END IF;

    -- A PS33MF change re-syncs the SKM record sharing this key triple.
    INSERT INTO SYNC_EVENTS (source_table, op, k_cp, k_yr, k_code)
    VALUES ('PS33MF', v_op, v_cp, v_yr, v_code);
END;
/

CREATE OR REPLACE TRIGGER trg_grbrf_aiud
AFTER INSERT OR UPDATE OR DELETE ON GRBRF
FOR EACH ROW
DECLARE
    v_op VARCHAR2(1);
    v_cp NUMBER;
    v_yr NUMBER;
    v_bn NUMBER;
BEGIN
    IF INSERTING THEN
        v_op := 'I'; v_cp := :NEW.GRBRCP; v_yr := :NEW.GRBRYR; v_bn := :NEW.BN;
    ELSIF UPDATING THEN
        v_op := 'U'; v_cp := :NEW.GRBRCP; v_yr := :NEW.GRBRYR; v_bn := :NEW.BN;
    ELSE
        v_op := 'D'; v_cp := :OLD.GRBRCP; v_yr := :OLD.GRBRYR; v_bn := :OLD.BN;
    END IF;

    INSERT INTO SYNC_EVENTS (source_table, op, k_cp, k_yr, k_bn)
    VALUES ('GRBRF', v_op, v_cp, v_yr, v_bn);
END;
/
