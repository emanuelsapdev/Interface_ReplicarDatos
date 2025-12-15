using SAPbobsCOM;
using System.Runtime.InteropServices;


public class InfraInstaller
{
    public static void InstallInCompany(Company cmp)
    {
        // UDTs
        CreateUDT(cmp, "@GNA_REP_CFG", "GNA_REP_CFG", BoUTBTableType.bott_NoObject);
        CreateUDT(cmp, "@GNA_REP_CHECK", "GNA_REP_CHECK", BoUTBTableType.bott_NoObject);
        CreateUDT(cmp, "@GNA_REP_LOG", "GNA_REP_LOG", BoUTBTableType.bott_NoObject);

        // ----- @GNA_REP_CFG -----
        CreateUDF(cmp, "@GNA_REP_CFG", "U_SrcDB", "Source DB", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_DstDB", "Dest DB", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_Table", "Table", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_FilterSQL", "Filter SQL", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_ExcludeCSV", "Exclude Fields", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_Active", "Active (Y/N)", BoFieldTypes.db_Alpha, 1);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_UseRepProperty", "Use Flag", BoFieldTypes.db_Alpha, 1);   // Y/N
        CreateUDF(cmp, "@GNA_REP_CFG", "U_RepRepPropertyCode", "Flag Code", BoFieldTypes.db_Alpha, 20);  // ej: P01 o U_Replicate

        // ----- @GNA_REP_CHECK -----
        CreateUDF(cmp, "@GNA_REP_CHECK", "U_RuleCode", "Rule Code", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@GNA_REP_CHECK", "U_LastDate", "Last Date", BoFieldTypes.db_Date);
        CreateUDF(cmp, "@GNA_REP_CHECK", "U_LastTime", "Last Time", BoFieldTypes.db_Alpha, 8);

        // ----- @GNA_REP_LOG -----
        CreateUDF(cmp, "@GNA_REP_LOG", "U_Rule", "Rule Code", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_Table", "Table", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_Key", "Key", BoFieldTypes.db_Alpha, 100);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_Status", "Status", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_Detail", "Detail", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_ExcludeKey", "ExcludeKey", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_LogDate", "LogDate", BoFieldTypes.db_Date);
        CreateUDF(cmp, "@GNA_REP_LOG", "U_LogTime", "LogTime", BoFieldTypes.db_Alpha, 8);

        // ----- UDF en OCRD para marcar replicación -----
        CreateUDF(cmp, "OCRD", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 1);
        CreateUDF(cmp, "OITM", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 1);
    }

    // ========== Helpers privados ==========

    private static void CreateUDT(Company c, string tableName, string description, BoUTBTableType type)
    {
        // tableName viene con @, el SDK quiere el nombre sin @
        string shortName = tableName.StartsWith("@") ? tableName.Substring(1) : tableName;

        var ut = (UserTablesMD)c.GetBusinessObject(BoObjectTypes.oUserTables);
        try
        {
            if (!ut.GetByKey(shortName))
            {
                ut.TableName = shortName;
                ut.TableDescription = description;
                ut.TableType = type;

                int ret = ut.Add();
                //if (ret != 0)
                //{
                //    c.GetLastError(out int code, out string msg);
                //    // -2035 = ya existe → lo ignoramos
                //    if (code != -2035)
                //        throw new Exception($"Error creando UDT {tableName}: {code} - {msg}");
                //}
            }
        }
        catch {}
    }

    private static void CreateUDF(Company c, string tableName, string alias, string desc,
                           BoFieldTypes type,  int size = 0, BoFldSubTypes subType = BoFldSubTypes.st_None)
    {
        if (FieldExists(c,tableName, alias))
            return;


        var uf = (UserFieldsMD)c.GetBusinessObject(BoObjectTypes.oUserFields);
        try
        {
            uf.TableName = tableName;
            uf.Name = alias.StartsWith("U_") ? alias.Substring(2) : alias;
            uf.Description = desc;
            uf.Type = type;
            uf.SubType = subType;

            if (size > 0)
                uf.EditSize = size;

            int ret = uf.Add();
            //if (ret != 0)
            //{
            //    c.GetLastError(out int code, out string msg);
            //    // -2035 = UDF ya existe → lo ignoramos
            //    if (code != -2035)
            //    {
            //        throw new Exception($"Error creando UDF {alias} en {tableName}: {code} - {msg}");
            //    }
            //}
        }
        finally
        {
            Marshal.ReleaseComObject(uf);
        }
    }
    private static bool FieldExists(Company c,string tableName, string fieldName)
    {
        var rs = (Recordset)c.GetBusinessObject(BoObjectTypes.BoRecordset);
        rs.DoQuery($@"
            SELECT 1 
              FROM CUFD 
             WHERE ""TableID"" = '{tableName.Replace("'", "''")}'
               AND ""AliasID"" = '{fieldName.Replace("'", "''")}'");

        bool exists = !rs.EoF;
        System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
        return exists;
    }
}