using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication
{
    public struct Checkpoint
    {
        public DateTime LastDate;  // solo fecha
        public TimeSpan LastTime;  // hora
    }

    public static class CheckpointService
    {
        public static Checkpoint LoadCheckpoint(Company cmp, string ruleCode)
        {
            var rs = (Recordset)cmp.GetBusinessObject(BoObjectTypes.BoRecordset);
            rs.DoQuery($@"SELECT ""U_LastDate"",""U_LastTime""
                          FROM ""@REP_CHECK""
                          WHERE ""Code"" = '{ruleCode}'");

            Checkpoint cp;

            if (rs.EoF)
            {
                // Primera vez: arrancamos bien atrás para que haga un full inicial
                cp.LastDate = new DateTime(2000, 1, 1);
                cp.LastTime = TimeSpan.Zero;

                rs.DoQuery(@"
                SELECT IFNULL(MAX(""DocEntry""), 0) + 1 AS NextDocEntry
                FROM ""@REP_CHECK""");

                int nextDoc = (int)rs.Fields.Item("NextDocEntry").Value;

                rs.DoQuery($@"
                INSERT INTO ""@REP_CHECK""
                    (""DocEntry"", ""Code"", ""Name"", ""U_LastDate"", ""U_LastTime"")
                VALUES ({nextDoc}, '{ruleCode}', '{ruleCode}', '{cp.LastDate:yyyy-MM-dd}', 0)");
            }
            else
            {
                var d = (DateTime)rs.Fields.Item("U_LastDate").Value;

                // U_LastTime viene como int (HHmmss)
                object rawTime = rs.Fields.Item("U_LastTime").Value;
                int tInt = 0;
                if (rawTime != null && rawTime != DBNull.Value)
                    tInt = Convert.ToInt32(rawTime);

                string tStr = tInt.ToString("D6"); // siempre 6 dígitos
                int hh = int.Parse(tStr.Substring(0, 2));
                int mm = int.Parse(tStr.Substring(2, 2));
                int ss = int.Parse(tStr.Substring(4, 2));

                cp.LastDate = d.Date;
                cp.LastTime = new TimeSpan(hh, mm, ss);
            }

            System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
            return cp;
        }

        public static void PersistCheckpoint(Company cmp, string ruleCode, Checkpoint cp)
        {
            var rs = (Recordset)cmp.GetBusinessObject(BoObjectTypes.BoRecordset);

            int intTime = ToIntTime(cp.LastTime); // hhmmss

            rs.DoQuery($@"
                        UPDATE ""@REP_CHECK""
                        SET ""U_LastDate"" = '{cp.LastDate:yyyy-MM-dd}',
                            ""U_LastTime"" = '{intTime}'
                        WHERE ""Code"" = '{ruleCode}'");

            System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
        }

        public static void UpdateFromRow(ref Checkpoint cp, Recordset rs, string dateField, string timeField)
        {
            var d = (DateTime)rs.Fields.Item(dateField).Value;

            // UpdateTime en DB suele ser int (HHmmss)
            string tRaw = rs.Fields.Item(timeField).Value.ToString();
            tRaw = tRaw.PadLeft(6, '0');
            int hh = int.Parse(tRaw.Substring(0, 2));
            int mm = int.Parse(tRaw.Substring(2, 2));
            int ss = int.Parse(tRaw.Substring(4, 2));
            var t = new TimeSpan(hh, mm, ss);

            if (d.Date > cp.LastDate || d.Date == cp.LastDate && t > cp.LastTime)
            {
                cp.LastDate = d.Date;
                cp.LastTime = t;
            }
        }

        public static int ToIntTime(TimeSpan t)
        {
            return t.Hours * 10000 + t.Minutes * 100 + t.Seconds;
        }
    }
}

