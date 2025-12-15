using Interface_ReplicarDatos.Replication.Models;
using Interface_ReplicarDatos.Replication.Services;
using SAPbobsCOM;
using System;

namespace Interface_ReplicarDatos.Replication
{
    public static class OitmReplicator
    {
        public static void Run(RepRule rule, IDiApiConnectionFactory factory)
        {
            Company src = null;
            Company dst = null;

            try
            {
                src = factory.Connect(rule.SrcDB);
                dst = factory.Connect(rule.DstDB);

                var cp = CheckpointService.LoadCheckpoint(src, rule.Code);

                var rs = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);

                string sql = $@"SELECT 
                            ""ItemCode"",
                            IFNULL(""U_Replicate"", 'N') AS ""U_Replicate"",
                            ""UpdateDate"",
                            ""UpdateTS"" 
                            FROM ""OITM"" 
                            WHERE (""UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}' 
                            OR (""UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND ""UpdateTS"" > {cp.LastTime}))";

                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                rs.DoQuery(sql);

                if (!dst.InTransaction)
                    dst.StartTransaction();

                var itmSrc = (Items)src.GetBusinessObject(BoObjectTypes.oItems);
                var itmDst = (Items)dst.GetBusinessObject(BoObjectTypes.oItems);

                while (!rs.EoF)
                {
                    string itemCode = rs.Fields.Item("ItemCode").Value.ToString();
                    string uRep = rs.Fields.Item("U_Replicate").Value.ToString();

                    if (rule.UseRepProperty && !string.IsNullOrWhiteSpace(rule.RepPropertyCode))
                    {
                        if (rule.RepPropertyCode == "U_Replicate" && uRep != "Y")
                        {
                            rs.MoveNext();
                            continue;
                        }
                    }

                    if (!itmSrc.GetByKey(itemCode))
                    {
                        LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itemCode,
                        status: "WARNING", detail: "No se pudo cargar el artículo origen OITM", excludeKey: null);
                        rs.MoveNext();
                        continue;
                    }

                    bool exists = itmDst.GetByKey(itemCode);
                    if (!exists)
                    {
                        itmDst.ItemCode = itmSrc.ItemCode;
                    }

                    // ================= CABECERA / DATOS GENERALES =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        RuleHelpers.SetIfAllowed(() => itmDst.ItemName = itmSrc.ItemName, "OITM.ItemName", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ForeignName = itmSrc.ForeignName, "OITM.FrgnName", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ItemsGroupCode = itmSrc.ItemsGroupCode, "OITM.ItmsGrpCod", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.InventoryItem = itmSrc.InventoryItem, "OITM.InvntItem", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesItem = itmSrc.SalesItem, "OITM.SellItem", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseItem = itmSrc.PurchaseItem, "OITM.PrchseItem", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.BarCode = itmSrc.BarCode, "OITM.CodeBars", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.Manufacturer = itmSrc.Manufacturer, "OITM.FirmCode", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ManageSerialNumbers = itmSrc.ManageSerialNumbers, "OITM.ManSerNum", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ManageBatchNumbers = itmSrc.ManageBatchNumbers, "OITM.ManBtchNum", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesUnit = itmSrc.SalesUnit, "OITM.SalUnitMsr", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseUnit = itmSrc.PurchaseUnit, "OITM.BuyUnitMsr", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.Valid = itmSrc.Valid, "OITM.validFor", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ValidFrom = itmSrc.ValidFrom, "OITM.validFrom", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ValidTo = itmSrc.ValidTo, "OITM.validTo", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ValidRemarks = itmSrc.ValidRemarks, "OITM.ValidComm", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.Frozen = itmSrc.Frozen, "OITM.frozenFor", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.FrozenFrom = itmSrc.FrozenFrom, "OITM.frozenFrom", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.FrozenTo = itmSrc.FrozenTo, "OITM.frozenTo", rule);
                    }, "OITM.FLAP_GENERAL", rule);

                    // ================= DATOS DE INVENTARIO =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Niveles y volúmenes
                        RuleHelpers.SetIfAllowed(() => itmDst.MinInventory = itmSrc.MinInventory, "OITM.MinLevel", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.MaxInventory = itmSrc.MaxInventory, "OITM.MaxLevel", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesUnitVolume = itmSrc.SalesUnitVolume, "OITM.SVolume", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseUnitVolume = itmSrc.PurchaseUnitVolume, "OITM.BVolume", rule);

                        // Cuentas contables
                        RuleHelpers.SetIfAllowed(() => itmDst.IncomeAccount  = itmSrc.IncomeAccount,  "OITM.IncomeAcct", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ExpanseAccount = itmSrc.ExpanseAccount, "OITM.ExpensAcct", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.ForeignExpensesAccount = itmSrc.ForeignExpensesAccount, "OITM.FrgnInAcct", rule);

                        // Impuestos / tipo
                        RuleHelpers.SetIfAllowed(() => itmDst.VatLiable = itmSrc.VatLiable, "OITM.VATLiable", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.WTLiable  = itmSrc.WTLiable,  "OITM.WTLiable",  rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.TaxType   = itmSrc.TaxType,   "OITM.TaxType",   rule);

                        // Unidades base / inventario
                        RuleHelpers.SetIfAllowed(() => itmDst.InventoryUOM = itmSrc.InventoryUOM, "OITM.InvntryUom", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.BaseUnitName = itmSrc.BaseUnitName, "OITM.BaseUnit",   rule);
                    }, "OITM.FLAP_INVENTARIO", rule);

                    // ================= DATOS DE VENTAS =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Grupo de IVA ventas
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesVATGroup = itmSrc.SalesVATGroup, "OITM.VatGourpSa", rule);

                        // Factores de venta: columnas reales SalFactor*
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesFactor1 = itmSrc.SalesFactor1, "OITM.SalFactor1", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesFactor2 = itmSrc.SalesFactor2, "OITM.SalFactor2", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesFactor3 = itmSrc.SalesFactor3, "OITM.SalFactor3", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.SalesFactor4 = itmSrc.SalesFactor4, "OITM.SalFactor4", rule);
                    }, "OITM.FLAP_SALES", rule);

                    // ================= DATOS DE COMPRAS =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Grupo de IVA compras
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseVATGroup = itmSrc.PurchaseVATGroup, "OITM.VatGroupPu", rule);

                        // Factores de compra: columnas reales PurFactor*
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseFactor1 = itmSrc.PurchaseFactor1, "OITM.PurFactor1", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseFactor2 = itmSrc.PurchaseFactor2, "OITM.PurFactor2", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseFactor3 = itmSrc.PurchaseFactor3, "OITM.PurFactor3", rule);
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseFactor4 = itmSrc.PurchaseFactor4, "OITM.PurFactor4", rule);
                    }, "OITM.FLAP_PURCHASE", rule);

                    // ================= PROPIEDADES =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        for (int i = 1; i <= 64; i++)
                        {
                            string fieldName = $"QryGroup{i}";
                            RuleHelpers.SetIfAllowed(() => itmDst.Properties[i] = itmSrc.Properties[i], $"OITM.{fieldName}", rule);
                        }
                    }, "OITM.FLAP_PROPERTIES", rule);


                    // Cuentas contables
                    RuleHelpers.SetIfAllowed(() => itmDst.IncomeAccount  = itmSrc.IncomeAccount,  "OITM.IncomeAcct", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.ExpanseAccount= itmSrc.ExpanseAccount,"OITM.ExpensAcct", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.ForeignExpensesAccount = itmSrc.ForeignExpensesAccount, "OITM.FrgnInAcct", rule); // si la propiedad existe

                    // Impuestos / tipo
                    RuleHelpers.SetIfAllowed(() => itmDst.VatLiable = itmSrc.VatLiable, "OITM.VATLiable", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.WTLiable = itmSrc.WTLiable, "OITM.WTLiable", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.TaxType  = itmSrc.TaxType,  "OITM.TaxType",  rule);

                    // Clasificación / tipo de ítem
                    RuleHelpers.SetIfAllowed(() => itmDst.ItemType = itmSrc.ItemType, "OITM.ItemType", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.ItemClass = itmSrc.ItemClass, "OITM.ItemClass", rule); // si existe
                    RuleHelpers.SetIfAllowed(() => itmDst.ItemCountryOrg = itmSrc.ItemCountryOrg, "OITM.CountryOrg", rule);

                    // Planificación / compras
                    RuleHelpers.SetIfAllowed(() => itmDst.PlanningSystem   = itmSrc.PlanningSystem,   "OITM.PlaningSys", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.ProcurementMethod= itmSrc.ProcurementMethod,"OITM.PrcrmntMtd", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.OrderIntervals    = itmSrc.OrderIntervals,    "OITM.OrdrIntrvl", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.OrderMultiple    = itmSrc.OrderMultiple,    "OITM.OrdrMulti",  rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.MinOrderQuantity = itmSrc.MinOrderQuantity, "OITM.MinOrdrQty", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.LeadTime        = itmSrc.LeadTime,         "OITM.LeadTime",   rule);

                    // Unidades base / inventario
                    RuleHelpers.SetIfAllowed(() => itmDst.InventoryUOM = itmSrc.InventoryUOM, "OITM.InvntryUom", rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.BaseUnitName = itmSrc.BaseUnitName, "OITM.BaseUnit",   rule);

                    // Otros campos comunes relacionados con logística
                    RuleHelpers.SetIfAllowed(() => itmDst.ShipType   = itmSrc.ShipType,   "OITM.ShipType",   rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.GLMethod   = itmSrc.GLMethod,   "OITM.GLMethod",   rule);
                    RuleHelpers.SetIfAllowed(() => itmDst.NoDiscounts= itmSrc.NoDiscounts,"OITM.NoDiscount", rule);

                    int ret = exists ? itmDst.Update() : itmDst.Add();

                    LogService.HandleDiApiResult(src, dst, ret, rule.Code, "OITM", itemCode);

                    CheckpointService.UpdateFromRow(ref cp, rs, "UpdateDate", "UpdateTS");

                    rs.MoveNext();
                }

                if (dst.InTransaction)
                    dst.EndTransaction(BoWfTransOpt.wf_Commit);

                CheckpointService.PersistCheckpoint(src, rule.Code, cp);

                System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(itmSrc);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(itmDst);
            }
            finally
            {
                factory.Disconnect(dst);
                factory.Disconnect(src);
            }
        }
    }
}