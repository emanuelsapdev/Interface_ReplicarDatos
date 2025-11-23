using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication.Services
{
    public struct Checkpoint
    {
        public DateTime LastDate;  // solo fecha
        public double LastTime;  // hora
    }

    public static class CheckpointService
    {
        public static Checkpoint LoadCheckpoint(Company cmp, string ruleCode)
        {
            var rs = (Recordset)cmp.GetBusinessObject(BoObjectTypes.BoRecordset);
            rs.DoQuery($@"SELECT ""U_LastDate"",IFNULL(""U_LastTime"", '0') AS ""U_LastTime""
                          FROM ""@GNA_REP_CHECK""
                          WHERE ""U_RuleCode"" = '{ruleCode}'");

            Checkpoint cp;

            if (rs.EoF)
            {
                // Primera vez: arrancamos bien atrás para que haga un full inicial
                cp.LastDate = new DateTime(2000, 1, 1);
                cp.LastTime = 0;

                string key = $"{DateTime.Now:FFFFFFF}"; //  @"(SELECT REPLACE_REGEXPR('[:|\-|\.| |]' IN CURRENT_TIMESTAMP WITH '') FROM DUMMY;)";
                rs.DoQuery($@"
                INSERT INTO ""@GNA_REP_CHECK""
                    (""Code"", ""Name"", ""U_RuleCode"",""U_LastDate"", ""U_LastTime"")
                VALUES ('{key}', '{key}', '{ruleCode}', '{cp.LastDate:yyyy-MM-dd}', '0')");
            }
            else
            {
                var d = (DateTime)rs.Fields.Item("U_LastDate").Value;

                // U_LastTime viene como int (HHmmss)
                string rawTime = rs.Fields.Item("U_LastTime").Value;

                double lastTime = double.Parse(rawTime);

                cp.LastDate = d.Date;
                cp.LastTime = lastTime;
            }

            System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
            return cp;
        }

        public static void PersistCheckpoint(Company cmp, string ruleCode, Checkpoint cp)
        {
            var rs = (Recordset)cmp.GetBusinessObject(BoObjectTypes.BoRecordset);


            rs.DoQuery($@"
                        UPDATE ""@GNA_REP_CHECK""
                        SET ""U_LastDate"" = '{cp.LastDate:yyyy-MM-dd}',
                            ""U_LastTime"" = '{cp.LastTime}'
                        WHERE ""U_RuleCode"" = '{ruleCode}'");

            System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
        }



        public static void UpdateFromRow(ref Checkpoint cp, Recordset rs, string dateField, string timeField)
        {
            var d = (DateTime)rs.Fields.Item(dateField).Value;

            // UpdateTime en DB suele ser int (HHmmss)
            string tRaw = rs.Fields.Item(timeField).Value.ToString();
            double timeUpdate = double.Parse(tRaw);

            if (d.Date > cp.LastDate || d.Date == cp.LastDate && timeUpdate > cp.LastTime)
            {
                cp.LastDate = d.Date;
                cp.LastTime = timeUpdate;
            }
        }

  
    }
}

