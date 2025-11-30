using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication.Services 

{ 
    public static class LogService
    {
        public static void WriteLog(Company src, string ruleCode, string table, string key, string status, string detail, string excludeKey)
        {
            var rs = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);
            try
            {

                ruleCode = (ruleCode ?? "").Replace("'", "''");
                table = (table ?? "").Replace("'", "''");
                key = (key ?? "").Replace("'", "''");
                status = (status ?? "").Replace("'", "''");
                detail = (detail ?? "").Replace("'", "''");
                excludeKey = (excludeKey ?? "").Replace("'", "''");

                string code = $"{DateTime.Now:FFFFFFF}";

                string sql = $@"
                INSERT INTO ""@GNA_REP_LOG""
                    (""Code"",""Name"",""U_Rule"",""U_Table"",""U_Key"",""U_Status"",""U_Detail"", ""U_ExcludeKey"",""U_LogDate"", ""U_LogTime"")
                VALUES
                    ('{code}','{code}', '{ruleCode}', '{table}', '{key}', '{status}', '{detail}', '{excludeKey}','{DateTime.Now:yyyy-MM-dd}', '{DateTime.Now:HHmmss}')";

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
                WriteLog(src, ruleCode, table, key, "OK", "", "");
            }
            else
            {
                dst.GetLastError(out int code, out string msg);
                WriteLog(src, ruleCode, table, key, "ERROR", $"{code} - {msg}", "");
            }
        }
    }
}
