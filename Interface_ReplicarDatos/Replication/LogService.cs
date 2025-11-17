using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication 

{ 
    public static class LogService
    {
        public static void WriteLog(Company src, string ruleCode, string table, string key, string status, string detail)
        {
            var rs = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);
            try
            {

                ruleCode = (ruleCode ?? "").Replace("'", "''");
                table = (table ?? "").Replace("'", "''");
                key = (key ?? "").Replace("'", "''");
                status = (status ?? "").Replace("'", "''");
                detail = (detail ?? "").Replace("'", "''");

                // 1) Busco el próximo DocEntry
                rs.DoQuery(@"
                SELECT IFNULL(MAX(""DocEntry""), 0) + 1 AS ""NextDocEntry""
                FROM ""@REP_LOG""");

                int nextDoc = Convert.ToInt32(rs.Fields.Item("NextDocEntry").Value);

                // 2) Inserto incluyendo DocEntry
                string sql = $@"
                INSERT INTO ""@REP_LOG""
                    (""DocEntry"",""U_Rule"",""U_Table"",""U_Key"",""U_Status"",""U_Detail"")
                VALUES
                    ({nextDoc}, '{ruleCode}', '{table}', '{key}', '{status}', '{detail}')";

                rs.DoQuery(sql);
            }
            finally
            {
                System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
            }
        }

        public static void HandleDiApiResult(Company src, Company dst, int retCode, string ruleCode, string table, string key)
        {
            if (retCode == 0)
            {
                WriteLog(src, ruleCode, table, key, "OK", "");
            }
            else
            {
                dst.GetLastError(out int code, out string msg);
                WriteLog(src, ruleCode, table, key, "ERROR", $"{code} - {msg}");
            }
        }
    }
}
