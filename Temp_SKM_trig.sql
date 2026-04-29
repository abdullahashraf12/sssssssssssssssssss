CREATE OR REPLACE TRIGGER trg_temp_skm_ai
AFTER INSERT OR UPDATE OR DELETE ON Temp_SKM
FOR EACH ROW
DECLARE
   v_msg VARCHAR2(4000);
BEGIN

   -- 🛑 امنع الـ loop لما Python يغيّر status بس
   IF UPDATING AND :OLD.status != :NEW.status THEN
      RETURN;
   END IF;

   -- 🧠 INSERT أو UPDATE
   IF INSERTING OR UPDATING THEN
      v_msg := '{<rec-mode>' || :NEW.rec_mode || '</rec-mode>' ||
               '<status>'   || :NEW.status   || '</status>'  ||
               '<id>'       || :NEW.id       || '</id>}';

   -- 🧠 DELETE (مفيش :NEW)
   ELSIF DELETING THEN
      v_msg := '{<rec-mode>DELETE</rec-mode>' ||
               '<status>'   || :OLD.status   || '</status>'  ||
               '<id>'       || :OLD.id       || '</id>}';
   END IF;

   -- 📩 إرسال الـ alert
   DBMS_ALERT.SIGNAL('temp_skm_insert', v_msg);

END;
/