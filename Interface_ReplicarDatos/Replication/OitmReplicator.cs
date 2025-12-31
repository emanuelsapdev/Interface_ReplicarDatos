using Interface_ReplicarDatos.Replication.Models;
using Interface_ReplicarDatos.Replication.Services;
using NLog.Targets.Wrappers;
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

                    if (!dst.InTransaction)
                        dst.StartTransaction();

                    //if (!itmSrc.GetByKey(itemCode))
                    //{
                    //    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itemCode,
                    //    status: "WARNING", detail: "No se pudo cargar el artículo origen OITM", excludeKey: null);
                    //    rs.MoveNext();
                    //    continue;
                    //}

                    bool exists = itmDst.GetByKey(itemCode);
                    if (!exists)
                    {
                        itmDst.ItemCode = itmSrc.ItemCode;
                    }

                    // ================= CABECERA / DATOS GENERALES =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        RuleHelpers.SetIfAllowed(() => itmDst.ItemName = itmSrc.ItemName, "OITM.ItemName", rule); // NOMBRE ARTICULO
                        RuleHelpers.SetIfAllowed(() => itmDst.ForeignName = itmSrc.ForeignName, "OITM.FrgnName", rule); // NOMBRE EXTRANJERO
                        RuleHelpers.SetIfAllowed(() => itmDst.ItemType = itmSrc.ItemType, "OITM.ItemType", rule); // CLASE DE ARTICULO
                        RuleHelpers.SetIfAllowed(() => // GRUPOS DE ARTICULOS
                        {
                            string? dstItemGrpCode = MasterDataMapper.MapByDescription(src, dst, table: "OITB", codeField: "ItmsGrpCod", descField: @"""ItmsGrpNam""", srcCode: itmSrc.ItemsGroupCode.ToString(), "", out string? srcItemGroupName);
                            if (dstItemGrpCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcItemGroupName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itmSrc.ItemCode, "WARNING", $"No se encontró mapeo para Grupo de Artículo '{srcItemGroupName}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.", "OITM.ItmsGrpCod");
                                }
                                return;
                            }
                            itmDst.ItemsGroupCode = int.Parse(dstItemGrpCode);

                        }, "OITM.ItmsGrpCod", rule);

                        RuleHelpers.SetIfAllowed(() => // GRUPOS DE UNIDADES DE MEDIDA 
                        {
                            string? dstUgpCode = MasterDataMapper.MapByDescription(src, dst, table: "OUGP", codeField: "UgpEntry", descField: @"""UgpCode""", srcCode: itmSrc.UoMGroupEntry.ToString(), "", out string? srcUgpCode);
                            if (dstUgpCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcUgpCode))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itmSrc.ItemCode, "WARNING", $"No se encontró mapeo para Grupo de Unidad de Medida '{srcUgpCode}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.", "OITM.UgpEntry");
                                }
                                return;
                            }
                            itmDst.UoMGroupEntry = int.Parse(dstUgpCode);

                        }, "OITM.UgpEntry", rule);

                        RuleHelpers.SetIfAllowed(() => itmDst.BarCode = itmSrc.BarCode, "OITM.CodeBars", rule); // CÓDIGO DE BARRAS

                        RuleHelpers.SetIfAllowed(() => // UNIDAD DE DETERMINACIÓN DE PRECIOS
                        {
                            string? dstPricingUnit = MasterDataMapper.MapByDescription(src, dst, table: "OUOM", codeField: "UomEntry", descField: @"""UomCode""", srcCode: itmSrc.PricingUnit.ToString(), "", out string? srcPricingUnit);
                            if (dstPricingUnit == null)
                            {
                                if (!string.IsNullOrEmpty(srcPricingUnit))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itmSrc.ItemCode, "WARNING", $"No se encontró mapeo para Unidad de determinacion de precios '{srcPricingUnit}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.", "OITM.PriceUnit");
                                }
                                return;
                            }
                            itmDst.PricingUnit = int.Parse(dstPricingUnit);

                        }, "OITM.PriceUnit", rule);



                        RuleHelpers.SetIfAllowed(() => itmDst.InventoryItem = itmSrc.InventoryItem, "OITM.InvntItem", rule); // ARTÍCULO DE INVENTARIO

                        RuleHelpers.SetIfAllowed(() => itmDst.SalesItem = itmSrc.SalesItem, "OITM.SellItem", rule); // ARTÍCULO DE VENTA

                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseItem = itmSrc.PurchaseItem, "OITM.PrchseItem", rule);  // ARTÍCULO DE COMPRA

                        RuleHelpers.SetIfAllowed(() => itmDst.WTLiable = itmSrc.WTLiable, "OITM.WTLiable", rule); // SUJETO RETENCIÓN

                        RuleHelpers.SetIfAllowed(() => itmDst.IndirectTax = itmSrc.IndirectTax, "OITM.IndirctTax", rule); // IMPUESTO INDIRECTO

                        RuleHelpers.SetIfAllowed(() => itmDst.NoDiscounts = itmSrc.NoDiscounts, "OITM.NoDiscount", rule); // SIN DESCUENTOS

                        RuleHelpers.SetIfAllowed(() => itmDst.Manufacturer = itmSrc.Manufacturer, "OITM.FirmCode", rule); // FABRICANTE

                        RuleHelpers.SetIfAllowed(() => itmDst.SWW = itmSrc.SWW, "OITM.SWW", rule); // ID ADICIONAL

                        RuleHelpers.SetIfAllowed(() => // FORMA DE ENVÍO
                        {
                            

                            string? dstShipType = MasterDataMapper.MapByDescription(src, dst, table: "OSHP", codeField: "TrnspCode", descField: @"""TrnspName""", srcCode: itmSrc.ShipType.ToString(), "", out string? srcShipTypeNam);
                            if (dstShipType == null)
                            {
                                if (!string.IsNullOrEmpty(srcShipTypeNam))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itmSrc.ItemCode, "WARNING", $"No se encontró mapeo para Forma de envío '{srcShipTypeNam}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.", "OITM.PriceUnit");
                                }
                                return;
                            }
                            itmDst.ShipType = int.Parse(dstShipType);

                        }, "OITM.ShipType", rule);

                        RuleHelpers.SetIfAllowed(() => itmDst.Valid = itmSrc.Valid, "OITM.validFor", rule); // ARTÍCULO VÁLIDO
                        RuleHelpers.SetIfAllowed(() => itmDst.ValidFrom = itmSrc.ValidFrom, "OITM.validFrom", rule); // VÁLIDO DESDE
                        RuleHelpers.SetIfAllowed(() => itmDst.ValidTo = itmSrc.ValidTo, "OITM.validTo", rule); // VÁLIDO HASTA
                        RuleHelpers.SetIfAllowed(() => itmDst.ValidRemarks = itmSrc.ValidRemarks, "OITM.ValidComm", rule); // COMENTARIOS VÁLIDO
                        RuleHelpers.SetIfAllowed(() => itmDst.Frozen = itmSrc.Frozen, "OITM.frozenFor", rule); // ARTÍCULO BLOQUEADO
                        RuleHelpers.SetIfAllowed(() => itmDst.FrozenFrom = itmSrc.FrozenFrom, "OITM.frozenFrom", rule); // BLOQUEADO DESDE
                        RuleHelpers.SetIfAllowed(() => itmDst.FrozenTo = itmSrc.FrozenTo, "OITM.frozenTo", rule); // BLOQUEADO HASTA

                    }, "OITM.FLAP_GENERAL", rule);

                    // ================= DATOS DE COMPRAS =================
                    RuleHelpers.SetIfAllowed(() =>
                    {

                        RuleHelpers.SetIfAllowed(() => {
                            string? dstSuppCardCode = MasterDataMapper.MapByDescription(src, dst, table: "OCRD", codeField: "CardCode", descField: @"""CardName""", srcCode: itmSrc.PreferredVendors.BPCode, "", out string? srcSuppCardName);
                            if (dstSuppCardCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcSuppCardName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itmSrc.ItemCode, "WARNING", $"No se encontró mapeo para Proveedor predeterminado '{srcSuppCardName}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.", "OITM.CardCode");
                                }
                                return;
                            }
                            itmDst.PreferredVendors.Add();
                            itmDst.PreferredVendors.BPCode = dstSuppCardCode;

                        }, "OITM.CardCode", rule); // CÓDIGO DE PROVEEDOR

                        RuleHelpers.SetIfAllowed(() => itmDst.SupplierCatalogNo = itmSrc.SupplierCatalogNo, "OITM.SuppCatNum", rule); // NÚMERO DE CATÁLOGO DEL PROVEEDOR

                        RuleHelpers.SetIfAllowed(() => // UNIDAD DE MEDIDA DE COMPRAS
                        {
                            string? dstBuyUnit = MasterDataMapper.MapByDescription(src, dst, table: "OUOM", codeField: "UomEntry", descField: @"""UomCode""", srcCode: itmSrc.PurchaseUnit.ToString(), "", out string? srcBuyUnit);
                            if (dstBuyUnit == null)
                            {
                                if (!string.IsNullOrEmpty(srcBuyUnit))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: itmSrc.ItemCode, "WARNING", $"No se encontró mapeo para Código de unidad de medida de compras '{srcBuyUnit}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.", "OITM.PriceUnit");
                                }
                                return;
                            }
                            itmDst.PurchaseUnit = dstBuyUnit;

                        }, "OITM.BuyUnitMsr", rule);

                        
                        RuleHelpers.SetIfAllowed(() => itmDst.ApTaxCode = itmSrc.ApTaxCode, "OITM.TaxCodeAP", rule); // CÓDIGO DE IMPUESTOS DE COMPRAS

                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseLengthUnit = itmSrc.PurchaseLengthUnit, "OITM.BLength1", rule); // UNIDAD DE LONGITUD DE COMPRAS
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseWidthUnit  = itmSrc.PurchaseWidthUnit,  "OITM.BLength2", rule); // UNIDAD DE ANCHURA DE COMPRAS
                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseHeightUnit = itmSrc.PurchaseHeightUnit, "OITM.BLength3", rule); // UNIDAD DE ALTURA DE COMPRAS

                        FALTA TERMINAR

                        /*
                            BLen1Unit
                            Blength2
                            BLen2Unit
                            BVolume
                            BVolUnit
                            BWeight1
                            BWght1Unit
                            BWeight2
                            BWght2Unit
                         */

                    }, "OITM.FLAP_PURCHASE", rule);


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