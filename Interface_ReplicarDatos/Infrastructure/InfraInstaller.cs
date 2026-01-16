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
        CreateUDF(cmp, "@GNA_REP_CFG", "U_TablesInvolved", "Tables Involved", BoFieldTypes.db_Alpha, 100);  // ej: OITM | OPLN | ITM1
        CreateUDF(cmp, "@GNA_REP_CFG", "U_FilterSQL", "Filter SQL", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_ExcludeCSV", "Exclude Fields", BoFieldTypes.db_Memo, 254);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_Active", "Active (Y/N)", BoFieldTypes.db_Alpha, 1);
        CreateUDF(cmp, "@GNA_REP_CFG", "U_UseRepProperty", "Use Flag", BoFieldTypes.db_Alpha, 1);   // Y/N
        CreateUDF(cmp, "@GNA_REP_CFG", "U_RepPropertyCode", "Flag Code", BoFieldTypes.db_Alpha, 20);  // ej: P01 o U_Replicate

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

        // ----- UDFs en tablas estandars -----
        CreateUDF(cmp, "OCRD", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 1); // Clientes y Proveedores

        CreateUDF(cmp, "OITM", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 1); // Artículos

        CreateUDF(cmp, "OPLN", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 1); // Listas de Precios

        CreateUDF(cmp, "ITM1", "U_Replicate", "Replicar (Y/N)", BoFieldTypes.db_Alpha, 8); // Precios de Artículos
        CreateUDF(cmp, "ITM1", "U_UpdateDate", "Fecha Actualización", BoFieldTypes.db_Date); // Precios de Artículos
        CreateUDF(cmp, "ITM1", "U_UpdateTS", "Hora Actualización", BoFieldTypes.db_Alpha, 8); // Precios de Artículos


        // ----- Datos iniciales en @GNA_REP_CFG -----
        var rs = (Recordset)cmp.GetBusinessObject(BoObjectTypes.BoRecordset);
        try
        {
            // PHXA > MUNDOBB > OCRD_PROV
            InsertRepCfgIfNotExists(
                rs,
                code: "PHXA>MUNDOBB>OCRD_PROV",
                name: "Proveedores PHXA a MUNDOBB",
                srcDb: "PHXA",
                dstDb: "MUNDOBB",
                tableName: "OCRD",
                filterSql: "\"OCRD\".\"CardType\"='S'",
                excludeCsv: "OCRD.GroupNum",
                active: "N",
                useRepProperty: string.Empty,
                repPropertyCode: string.Empty, 
                tablesInvolved: string.Empty);

            // PHXA > ML > OCRD_PROV
            InsertRepCfgIfNotExists(
                rs,
                code: "PHXA>ML>OCRD_PROV",
                name: "Proveedores PHXA a ML",
                srcDb: "PHXA",
                dstDb: "ML",
                tableName: "OCRD",
                filterSql: "\"OCRD\".\"CardType\"='S'",
                excludeCsv: string.Empty,
                active: "N",
                useRepProperty: string.Empty,
                repPropertyCode: string.Empty,
                tablesInvolved: string.Empty);

            // PHXA > PHXB > OCRD_PROV
            InsertRepCfgIfNotExists(
                rs,
                code: "PHXA>PHXB>OCRD_PROV",
                name: "Proveedores PHXA a PHXB",
                srcDb: "PHXA",
                dstDb: "PHXB",
                tableName: "OCRD",
                filterSql: "\"OCRD\".\"CardType\"='S'",
                excludeCsv: "OCRD.GroupNum",
                active: "N",
                useRepProperty: "Y",
                repPropertyCode: "U_Replicate",
                tablesInvolved: string.Empty);

            // PHXA > PHXB > OCRD_CLI
            InsertRepCfgIfNotExists(
                rs,
                code: "PHXA>PHXB>OCRD_CLI",
                name: "Clientes PHXA a PHXB",
                srcDb: "PHXA",
                dstDb: "PHXB",
                tableName: "OCRD",
                filterSql: "\"OCRD\".\"CardType\"='C'",
                excludeCsv: string.Empty,
                active: "N",
                useRepProperty: "Y",
                repPropertyCode: "U_Replicate",
                tablesInvolved: string.Empty);

            // PHXA > PHXB > OITM
            InsertRepCfgIfNotExists(
                rs,
                code: "PHXA>PHXB>OITM",
                name: "Articulos PHXA a PHXB",
                srcDb: "PHXA",
                dstDb: "PHXB",
                tableName: "OITM",
                filterSql: string.Empty,
                excludeCsv: string.Empty,
                active: "Y",
                useRepProperty: "Y",
                repPropertyCode: "U_Replicate",
                tablesInvolved: string.Empty
                );

            // PHXA > PHXB > ITM1
            InsertRepCfgIfNotExists(
                rs,
                code: "PHXA>PHXB>ITM1",
                name: "Lista de Precios PHXA a PHXB",
                srcDb: "PHXA",
                dstDb: "PHXB",
                tableName: "ITM1",
                filterSql: string.Empty,
                excludeCsv: string.Empty,
                active: "Y",
                useRepProperty: "Y",
                repPropertyCode: "U_Replicate",
                tablesInvolved: "OPLN | OITM");

            // Crear trigger de auditoria sobre ITM1 para mantener U_UpdateDate / U_UpdateTS
            CreateItm1UpdateTrigger(rs);
        }
        finally
        {
            Marshal.ReleaseComObject(rs);
        }
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
            }
        }
        catch { }
    }

    private static void CreateUDF(Company c, string tableName, string alias, string desc,
                           BoFieldTypes type, int size = 0, BoFldSubTypes subType = BoFldSubTypes.st_None)
    {
        if (FieldExists(c, tableName, alias))
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
        }
        finally
        {
            Marshal.ReleaseComObject(uf);
        }
    }
    private static bool FieldExists(Company c, string tableName, string fieldName)
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

    public static void InsertRepCfgIfNotExists(
        Recordset oRec,
        string code,
        string name,
        string srcDb,
        string dstDb,
        string tableName,
        string filterSql,
        string excludeCsv,
        string active,
        string useRepProperty,
        string repPropertyCode,
        string tablesInvolved
    )
    {
        // Escapar comillas simples para SQL HANA
        static string Esc(string? s) => (s ?? string.Empty).Replace("'", "''");

        string sql = $@"
            INSERT INTO ""@GNA_REP_CFG""
            (
                ""Code"",
                ""Name"",
                ""U_SrcDB"",
                ""U_DstDB"",
                ""U_Table"",
                ""U_FilterSQL"",
                ""U_ExcludeCSV"",
                ""U_Active"",
                ""U_UseRepProperty"",
                ""U_RepPropertyCode"",
                ""U_TablesInvolved""
            )
            SELECT
                '{Esc(code)}',
                '{Esc(name)}',
                '{Esc(srcDb)}',
                '{Esc(dstDb)}',
                '{Esc(tableName)}',
                '{Esc(filterSql)}',
                '{Esc(excludeCsv)}',
                '{Esc(active)}',
                '{Esc(useRepProperty)}',
                '{Esc(repPropertyCode)}',
                '{Esc(tablesInvolved)}'
            FROM DUMMY
            WHERE NOT EXISTS
            (
                SELECT 1
                FROM ""@GNA_REP_CFG""
                WHERE ""Code"" = '{Esc(code)}'
            );";

        oRec.DoQuery(sql);
    }

    /// <summary>
    /// Crea el trigger TRG_GNAEA_ITM1_UPDATE si no existe, para mantener los campos
    /// U_UpdateDate y U_UpdateTS de ITM1 en cada actualización de línea de precio.
    /// </summary>
    private static void CreateItm1UpdateTrigger(Recordset rs)
    {
        // Comprobar si el trigger ya existe (HANA: SYS.TRIGGERS)
        rs.DoQuery(@"SELECT 1 FROM SYS.TRIGGERS WHERE TRIGGER_NAME = 'TRG_GNAEA_ITM1_UPDATE'");
        if (!rs.EoF)
        {
            return; // Ya existe, no hacemos nada
        }

        string triggerSql = @"
                            CREATE OR REPLACE TRIGGER TRG_GNAEA_ITM1_UPDATE 
						    BEFORE UPDATE ON ITM1
						    REFERENCING NEW ROW AS N
                            FOR EACH ROW 
                            BEGIN
						    N.""U_UpdateDate"" := CURRENT_DATE;
						    N.""U_UpdateTS""   := TO_VARCHAR(CURRENT_TIME, 'HH24MISS');
END;";

        rs.DoQuery(triggerSql);
    }
}



