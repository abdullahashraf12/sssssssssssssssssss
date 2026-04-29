CREATE OR REPLACE PROCEDURE fill_zho IS
   -- Counter for the SKM loop
   v_counter NUMBER := 0;

   -- Cursor for SK1MF + PS33MF (joined, ordered)
   CURSOR skm_cur IS
      SELECT s.SK1MCP,
             s.SK1MYR,
             s.SK1M1,
             s.SK1M2,
             s.SK1M3,
             s.SK1M9,
             s.SK1M11,
             s.SK1M12,
             s.SK1M13,
             s.SK1M14,
             s.SK1M16,
             s.SK1M17,
             s.SK1M18,
             s.SK1M19,
             s.SK1M20,
             s.SK1M21,
             s.SK1M22,
             s.SK1M24,
             s.SK1M29,
             s.SK1M31,
             s.SK1M32,
             s.SK1M33,
             s.SK1M34,
             s.SK1M36,
             s.SK1M37,
             s.SK1M39,
             s.SK1M40,
             s.SK1M41,
             s.SK1M261,
             p.PS33M2,
             p.PS33M4
      FROM   sk1mf s, ps33mf p
      WHERE  p.PS33MCP = s.SK1MCP
        AND  p.PS33MYR = s.SK1MYR
        AND  p.PS33M1  = s.SK1M1
      ORDER BY s.SK1MCP, s.SK1MYR, s.SK1M1;

   -- Cursor for GRBRF (all rows, no join)
   CURSOR grb_cur IS
      SELECT GRBRCP,
             GRBRYR,
             BN,
             GRBR2,
             GRBR3
      FROM   grbrf;

BEGIN
   -- 1) Load Temp_SKM (first 30 rows only)
   FOR rec IN skm_cur LOOP
      EXIT WHEN v_counter >= 30;   -- stop after 30 inserts
      v_counter := v_counter + 1;

      INSERT INTO Temp_SKM (
         SK1MCP, SK1MYR, SK1M1, SK1M2, SK1M3, SK1M9, SK1M11, SK1M12, SK1M13,
         SK1M14, SK1M16, SK1M17, SK1M18, SK1M19, SK1M20, SK1M21, SK1M22,
         SK1M24, SK1M29, SK1M31, SK1M32, SK1M33, SK1M34, SK1M36, SK1M37,
         SK1M39, SK1M40, SK1M41, SK1M261, PS33M2, PS33M4
      ) VALUES (
         rec.SK1MCP, rec.SK1MYR, rec.SK1M1, rec.SK1M2, rec.SK1M3, rec.SK1M9,
         rec.SK1M11, rec.SK1M12, rec.SK1M13, rec.SK1M14, rec.SK1M16, rec.SK1M17,
         rec.SK1M18, rec.SK1M19, rec.SK1M20, rec.SK1M21, rec.SK1M22, rec.SK1M24,
         rec.SK1M29, rec.SK1M31, rec.SK1M32, rec.SK1M33, rec.SK1M34, rec.SK1M36,
         rec.SK1M37, rec.SK1M39, rec.SK1M40, rec.SK1M41, rec.SK1M261,
         rec.PS33M2, rec.PS33M4
      );
   END LOOP;

   -- 2) Load Temp_GRB (all rows, no limit)
   FOR rec IN grb_cur LOOP
      INSERT INTO Temp_GRB (
         GRBRCP, GRBRYR, BN, GRBR2, GRBR3
      ) VALUES (
         rec.GRBRCP, rec.GRBRYR, rec.BN, rec.GRBR2, rec.GRBR3
      );
   END LOOP;

   COMMIT;
END fill_zho;
/