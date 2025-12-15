using Interface_ReplicarDatos.Replication.Models;
using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication
{
    public static class RepRuleLoader
    {
        public static List<RepRule> LoadActiveRules(Company cfgCompany, string tableFilter = null)
        {
            var rules = new List<RepRule>();
            var rs = (Recordset)cfgCompany.GetBusinessObject(BoObjectTypes.BoRecordset);

            string sql = @"
                        SELECT ""Code"",
                               ""U_SrcDB"",""U_DstDB"",""U_Table"",
                               ""U_FilterSQL"",""U_ExcludeCSV"",""U_Active"",
                               ""U_UseRepProperty"",""U_RepPropertyCode""
                        FROM ""@GNA_REP_CFG""
                        WHERE ""U_Active"" = 'Y'";

            if (!string.IsNullOrWhiteSpace(tableFilter))
            {
                sql += $@" AND ""U_Table"" = '{tableFilter}'";
            }

            rs.DoQuery(sql);

            while (!rs.EoF)
            {
                var r = new RepRule
                {
                    Code = rs.Fields.Item("Code").Value.ToString(),
                    SrcDB = rs.Fields.Item("U_SrcDB").Value.ToString(),
                    DstDB = rs.Fields.Item("U_DstDB").Value.ToString(),
                    Table = rs.Fields.Item("U_Table").Value.ToString(),
                    FilterSQL = rs.Fields.Item("U_FilterSQL").Value.ToString(),
                    ExcludeCSV = rs.Fields.Item("U_ExcludeCSV").Value.ToString(),
                    Active = rs.Fields.Item("U_Active").Value.ToString() == "Y",
                    UseRepProperty = rs.Fields.Item("U_UseRepProperty").Value.ToString() == "Y",
                    RepPropertyCode = rs.Fields.Item("U_RepPropertyCode").Value.ToString()
                };

                rules.Add(r);
                rs.MoveNext();
            }

            System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
            return rules;
        }
    }
}
