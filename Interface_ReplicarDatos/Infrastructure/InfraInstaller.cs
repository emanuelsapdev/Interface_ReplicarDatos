using SAPbobsCOM;
using System.Runtime.InteropServices;

public interface IInfraInstaller
{
    void InstallInCompany(Company cmp);
}

public class InfraInstaller : IInfraInstaller
{
    public void InstallInCompany(Company cmp)
    {
        // UDTs
        CreateUDT(cmp, "@REP_CFG", "REP_CFG", BoUTBTableType.bott_MasterData);
        CreateUDT(cmp, "@REP_CHECK", "REP_CHECK", BoUTBTableType.bott_MasterData);
        CreateUDT(cmp, "@REP_LOG", "REP_LOG", BoUTBTableType.bott_Document);
        CreateUDT(cmp, "@REP_FMAP", "REP_FIELD_MAP", BoUTBTableType.bott_MasterData);

        // ----- @REP_CFG -----
        CreateUDF(cmp, "@REP_CFG", "U_SrcDB", "Source DB", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@REP_CFG", "U_DstDB", "Dest DB", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@REP_CFG", "U_Table", "Table", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@REP_CFG", "U_FilterSQL", "Filter SQL", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@REP_CFG", "U_ExcludeCSV", "Exclude Fields", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@REP_CFG", "U_AssignJSON", "Assign JSON", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@REP_CFG", "U_Active", "Active (Y/N)", BoFieldTypes.db_Alpha, 1);

        // Campos específicos del audio (tipo BP y marca de propiedad)
        CreateUDF(cmp, "@REP_CFG", "U_RepBPType", "BP Type (P/C/B)", BoFieldTypes.db_Alpha, 1);   // P=Prov, C=Cli, B=Ambos
        CreateUDF(cmp, "@REP_CFG", "U_UseBPProperty", "Use BP Flag", BoFieldTypes.db_Alpha, 1);   // Y/N
        CreateUDF(cmp, "@REP_CFG", "U_BPPropertyCode", "BP Flag Code", BoFieldTypes.db_Alpha, 20);  // ej: P01 o U_Replicate

        // ----- @REP_CHECK -----
        CreateUDF(cmp, "@REP_CHECK", "U_LastDate", "Last Date", BoFieldTypes.db_Date);
        CreateUDF(cmp, "@REP_CHECK", "U_LastTime", "Last Time", BoFieldTypes.db_Date, 0, BoFldSubTypes.st_Time);

        // ----- @REP_LOG -----
        CreateUDF(cmp, "@REP_LOG", "U_Rule", "Rule Code", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@REP_LOG", "U_Table", "Table", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@REP_LOG", "U_Key", "Key", BoFieldTypes.db_Alpha, 100);
        CreateUDF(cmp, "@REP_LOG", "U_Status", "Status", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@REP_LOG", "U_Detail", "Detail", BoFieldTypes.db_Memo, 254);

        // ----- @REP_FMAP ----- (mapeo de valores por base)
        CreateUDF(cmp, "@REP_FMAP", "U_FromDB", "From DB", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@REP_FMAP", "U_ToDB", "To DB", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@REP_FMAP", "U_Table", "Table", BoFieldTypes.db_Alpha, 20);
        CreateUDF(cmp, "@REP_FMAP", "U_Field", "Field", BoFieldTypes.db_Alpha, 50);
        CreateUDF(cmp, "@REP_FMAP", "U_SourceVal", "Source Value", BoFieldTypes.db_Alpha, 100);
        CreateUDF(cmp, "@REP_FMAP", "U_TargetVal", "Target Value", BoFieldTypes.db_Alpha, 100);
        CreateUDF(cmp, "@REP_FMAP", "U_Fixed", "Fixed (Y/N)", BoFieldTypes.db_Alpha, 1);

        // ----- UDF en OCRD para marcar replicación -----
        CreateUDF(cmp, "OCRD", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 1);
    }

    // ========== Helpers privados ==========

    private void CreateUDT(Company c, string tableName, string description, BoUTBTableType type)
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
                if (ret != 0)
                {
                    c.GetLastError(out int code, out string msg);
                    // -2035 = ya existe → lo ignoramos
                    if (code != -2035)
                        throw new Exception($"Error creando UDT {tableName}: {code} - {msg}");
                }
            }
        }
        finally
        {
            Marshal.ReleaseComObject(ut);
        }
    }

    private void CreateUDF(Company c, string tableName, string alias, string desc,
                           BoFieldTypes type,  int size = 0, BoFldSubTypes subType = BoFldSubTypes.st_None)
    {
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
            if (ret != 0)
            {
                c.GetLastError(out int code, out string msg);
                // -2035 = UDF ya existe → lo ignoramos
                if (code != -2035)
                    throw new Exception($"Error creando UDF {alias} en {tableName}: {code} - {msg}");
            }
        }
        finally
        {
            Marshal.ReleaseComObject(uf);
        }
    }
}