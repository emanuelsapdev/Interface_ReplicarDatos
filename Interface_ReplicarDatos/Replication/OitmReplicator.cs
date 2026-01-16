using Interface_ReplicarDatos.Replication.Models;
using Interface_ReplicarDatos.Replication.Services;
using NLog.Targets.Wrappers;
using SAPbobsCOM;
using System;

namespace Interface_ReplicarDatos.Replication
{
    /// <summary>
    /// Replica artículos (tabla OITM) desde una compañía origen a una compañía destino
    /// de SAP Business One, aplicando reglas de mapeo, filtros y checkpoints de fecha/hora.
    /// </summary>
    public static class OitmReplicator
    {
        /// <summary>
        /// Ejecuta la replicación de artículos según la regla indicada:
        /// - Conecta a las bases origen/destino.
        /// - Calcula el checkpoint (última fecha/hora replicada).
        /// - Lee los Items modificados desde ese checkpoint.
        /// - Mapea y asigna campos permitidos según la regla.
        /// - Ejecuta Add/Update en destino y maneja transacciones.
        /// - Actualiza y persiste el checkpoint.
        /// </summary>
        /// <param name="rule">Regla de replicación (origen, destino, tabla, filtros, flags, etc.).</param>
        /// <param name="factory">Fábrica para crear conexiones DI-API a SAP B1.</param>
        public static void Run(RepRule rule, IDiApiConnectionFactory factory)
        {
            Company src = null;
            Company dst = null;

            try
            {
                // Conectar a compañías origen y destino
                src = factory.Connect(rule.SrcDB);
                dst = factory.Connect(rule.DstDB);

                // Cargar último checkpoint de replicación para esta regla
                var cp = CheckpointService.LoadCheckpoint(src, rule.Code);

                // Recordset origen usado para obtener los ItemCode modificados
                var rs = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);

                // Consulta de artículos modificados desde el último checkpoint
                // Se filtra por UpdateDate/UpdateTS y opcionalmente por propiedad de replicación y SQL adicional de la regla.
                string sql = $@"SELECT 
                            ""ItemCode"",
                            ""UpdateDate"",
                            ""UpdateTS""
                            FROM ""OITM"" OITM
                            WHERE (OITM.""UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}' 
                            OR (OITM.""UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND OITM.""UpdateTS"" > {cp.LastTime}))";

                // Si la regla usa propiedad de replicación y el flag es U_Replicate, filtramos por U_Replicate en OITM
                if (rule.UseRepProperty && !string.IsNullOrWhiteSpace(rule.RepPropertyCode))
                {
                    sql += @$" AND IFNULL(OITM.""{rule.RepPropertyCode}"", 'Y') = 'Y'";
                }

                // Filtro adicional definido en la regla (WHERE extra)
                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                rs.DoQuery(sql);

                // Objetos Items de origen y destino para leer/escribir datos de OITM
                var itmSrc = (Items)src.GetBusinessObject(BoObjectTypes.oItems);
                var itmDst = (Items)dst.GetBusinessObject(BoObjectTypes.oItems);

                // Recorremos cada ItemCode pendiente de replicar
                while (!rs.EoF)
                {
                    string itemCode = rs.Fields.Item(0).Value;
                    itmSrc.GetByKey(itemCode);

                    // Cada artículo se procesa dentro de una transacción propia en la base destino
                    if (!dst.InTransaction)
                        dst.StartTransaction();

                    bool exists = itmDst.GetByKey(itemCode);
                    if (!exists)
                    {
                        // Si el artículo no existe en destino, se inicializa con el mismo código
                        itmDst.ItemCode = itmSrc.ItemCode;
                    }

                    // ================= CABECERA / DATOS GENERALES =================
                    // Cada bloque se envuelve en RuleHelpers.SetIfAllowed para:
                    // - Respetar los campos excluidos en la regla (ExcludeFields).
                    // - Permitir asignaciones forzadas desde AssignJSON.
                    // - Centralizar el control de qué campos se pueden modificar.
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        RuleHelpers.SetIfAllowed(() => itmDst.ItemName = itmSrc.ItemName, "OITM.ItemName", rule); // NOMBRE ARTICULO
                        RuleHelpers.SetIfAllowed(() => itmDst.ForeignName = itmSrc.ForeignName, "OITM.FrgnName", rule); // NOMBRE EXTRANJERO
                        RuleHelpers.SetIfAllowed(() => itmDst.ItemType = itmSrc.ItemType, "OITM.ItemType", rule); // CLASE DE ARTICULO
                        RuleHelpers.SetIfAllowed(() => // GRUPOS DE ARTICULOS
                        {
                            // Mapeo de grupo de artículos entre compañías por descripción (OITB.ItmsGrpNam)
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
                            // Mapeo de grupo de UoM por descripción (OUGP.UgpCode)
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
                            // Mapeo de unidad de determinación de precios (OUOM.UomCode)
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

                        RuleHelpers.SetIfAllowed(() => itmDst.PurchaseItem = itmSrc.PurchaseItem, "OITM.PrchseItem", rule); // ARTÍCULO DE COMPRA

                        RuleHelpers.SetIfAllowed(() => itmDst.WTLiable = itmSrc.WTLiable, "OITM.WTLiable", rule); // SUJETO RETENCIÓN

                        RuleHelpers.SetIfAllowed(() => itmDst.IndirectTax = itmSrc.IndirectTax, "OITM.IndirctTax", rule); // IMPUESTO INDIRECTO

                        RuleHelpers.SetIfAllowed(() => itmDst.NoDiscounts = itmSrc.NoDiscounts, "OITM.NoDiscount", rule); // SIN DESCUENTOS

                        RuleHelpers.SetIfAllowed(() => itmDst.Manufacturer = itmSrc.Manufacturer, "OITM.FirmCode", rule); // FABRICANTE

                        RuleHelpers.SetIfAllowed(() => itmDst.SWW = itmSrc.SWW, "OITM.SWW", rule); // ID ADICIONAL

                        RuleHelpers.SetIfAllowed(() => // FORMA DE ENVÍO
                        {
                            // Mapeo de forma de envío por descripción (OSHP.TrnspName)
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
                        // Proveedor predeterminado (Preferred Vendor simple)
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            string srcBpCode = itmSrc.PreferredVendors.BPCode;
                            if (string.IsNullOrEmpty(srcBpCode))
                                return;

                            // Mapeo de proveedor predeterminado entre compañías (OCRD.CardCode/CardName)
                            string? dstSuppCardCode = MasterDataMapper.MapByDescription(
                                src, dst,
                                table: "OCRD",
                                codeField: "CardCode",
                                descField: @"""CardName""",
                                srcCode: srcBpCode,
                                extensionWhereSQL: string.Empty,
                                out string? srcSuppCardName);

                            if (dstSuppCardCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcSuppCardName))
                                {
                                    LogService.WriteLog(
                                        src,
                                        rule.Code,
                                        rule.Table,
                                        itmSrc.ItemCode,
                                        "WARNING",
                                        $"No se encontró mapeo para Proveedor predeterminado '{srcSuppCardName}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.",
                                        "OITM.CardCode");
                                }
                                return;
                            }

                            itmDst.PreferredVendors.Add();
                            itmDst.PreferredVendors.BPCode = dstSuppCardCode;

                        }, "OITM.CardCode", rule);

                        // Número de catálogo del fabricante
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.SupplierCatalogNo = itmSrc.SupplierCatalogNo,
                            "OITM.SuppCatNum",
                            rule);

                        // Unidad de medida de compras (texto, no Entry)
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            string srcBuyUnit = itmSrc.PurchaseUnit;
                            if (string.IsNullOrEmpty(srcBuyUnit))
                                return;

                            // Si usas mismos códigos de UOM entre bases, puedes copiar tal cual:
                            itmDst.PurchaseUnit = srcBuyUnit;


                            // Mapeo de unidad de medida de compras (OUOM.UomCode)
                            string? dstBuyUnit = MasterDataMapper.MapByDescription(
                                src, dst,
                                table: "OUOM",
                                codeField: "UomCode",
                                descField: @"""UomCode""",
                                srcCode: srcBuyUnit,
                                extensionWhereSQL: string.Empty,
                                out string? srcBuyUnitName);

                            if (dstBuyUnit == null)
                            {
                                if (!string.IsNullOrEmpty(srcBuyUnitName))
                                {
                                    LogService.WriteLog(
                                        src,
                                        rule.Code,
                                        rule.Table,
                                        itmSrc.ItemCode,
                                        "WARNING",
                                        $"No se encontró mapeo para Unidad de medida de compras '{srcBuyUnitName}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.",
                                        "OITM.BuyUnitMsr");
                                }
                                return;
                            }

                            itmDst.PurchaseUnit = dstBuyUnit; // Asignar la unidad de medida mapeada


                        }, "OITM.BuyUnitMsr", rule);

                        // Código de impuestos de compras
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.ApTaxCode = itmSrc.ApTaxCode,
                            "OITM.TaxCodeAP",
                            rule);

                        // Grupo de aduanas
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.CustomsGroupCode = itmSrc.CustomsGroupCode,
                            "OITM.CustomPer",
                            rule);

                        // Longitud / Ancho / Altura / Volumen / Peso de compras:

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.PurchaseLengthUnit;
                            if (srcVal ==0)
                                return;
                            itmDst.PurchaseLengthUnit = srcVal;
                        }, "OITM.BLength1", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.PurchaseWidthUnit;
                            if (srcVal ==0)
                                return;
                            itmDst.PurchaseWidthUnit = srcVal;
                        }, "OITM.BWidth1", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.PurchaseHeightUnit;
                            if (srcVal ==0)
                                return;
                            itmDst.PurchaseHeightUnit = srcVal;
                        }, "OITM.BHeight1", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.PurchaseUnitVolume;
                            if (srcVal ==0)
                                return;
                            itmDst.PurchaseUnitVolume = srcVal;
                        }, "OITM.BVolume", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.PurchaseWeightUnit1;
                            if (srcVal ==0)
                                return;
                            itmDst.PurchaseWeightUnit1 = srcVal;
                        }, "OITM.BWeight1", rule);


                    }, "OITM.FLAP_PURCHASE", rule);

                    // ================= DATOS DE VENTAS =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // 1) Indicador de IVA (grupo de IVA ventas)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.SalesVATGroup = itmSrc.SalesVATGroup,
                            "OITM.VatGourpSa",
                            rule);

                        // 2) Código / nombre unidad de medida de ventas (SalUnitMsr)
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            string srcSalesUnit = itmSrc.SalesUnit;
                            if (string.IsNullOrEmpty(srcSalesUnit))
                                return;

                            // Mapeo de unidad de medida de ventas (OUOM.UomCode)
                            string? dstSalesUnit = MasterDataMapper.MapByDescription(
                                src,
                                dst,
                                table: "OUOM",
                                codeField: "UomCode",
                                descField: @"""UomCode""",
                                srcCode: srcSalesUnit,
                                extensionWhereSQL: string.Empty,
                                out string? srcSalesUnitName);

                            if (dstSalesUnit == null)
                            {
                                if (!string.IsNullOrEmpty(srcSalesUnitName))
                                {
                                    LogService.WriteLog(
                                        src,
                                        rule.Code,
                                        rule.Table,
                                        itmSrc.ItemCode,
                                        "WARNING",
                                        $"No se encontró mapeo para Unidad de medida de ventas '{srcSalesUnitName}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.",
                                        "OITM.SalUnitMsr");
                                }
                                return;
                            }

                            itmDst.SalesUnit = dstSalesUnit;

                        }, "OITM.SalUnitMsr", rule);

                        // 3) Artículos por unidad de ventas (NumInSale)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.SalesItemsPerUnit = itmSrc.SalesItemsPerUnit,
                            "OITM.NumInSale",
                            rule);

                        // 4) Clase de paquete de ventas / unidad paquete
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.SalesPackagingUnit = itmSrc.SalesPackagingUnit,
                            "OITM.SalPackMsr",
                            rule);

                        // 5) Cantidad por paquete de ventas
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.SalesQtyPerPackUnit = itmSrc.SalesQtyPerPackUnit,
                            "OITM.SalPackUn",
                            rule);

                        // 6) Longitud / Ancho / Altura / Volumen / Peso de ventas
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.SalesLengthUnit;
                            if (srcVal ==0)
                                return;
                            itmDst.SalesLengthUnit = srcVal;
                        }, "OITM.SLength1", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.SalesUnitWidth;
                            if (srcVal ==0)
                                return;
                            itmDst.SalesUnitWidth = srcVal;
                        }, "OITM.SWidth1", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.SalesUnitHeight1;
                            if (srcVal ==0)
                                return;
                            itmDst.SalesUnitHeight1 = srcVal;
                        }, "OITM.SHeight1", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.SalesUnitVolume;
                            if (srcVal ==0)
                                return;
                            itmDst.SalesUnitVolume = srcVal;
                        }, "OITM.SVolume", rule);

                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.SalesWeightUnit1;
                            if (srcVal ==0)
                                return;
                            itmDst.SalesWeightUnit1 = srcVal;
                        }, "OITM.SWeight1", rule);
                    }, "OITM.FLAP_SALES", rule);

                    // ================= DATOS DE INVENTARIO =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Unidad de medida de inventario (código)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.InventoryUOM = itmSrc.InventoryUOM,
                            "OITM.InvntryUom",
                            rule);

                        // Peso base (si usas el peso de ventas como peso inventario)
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcVal = itmSrc.InventoryWeight;
                            if (srcVal ==0)
                                return;
                            itmDst.InventoryWeight = srcVal;
                        }, "OITM.IWeight1", rule);

                        // Unidad de medida de recuento de inventario (Entry)
                        RuleHelpers.SetIfAllowed(() =>
                        {

                            string srcCntUomEntry = itmSrc.DefaultCountingUoMEntry.ToString();
                            if (string.IsNullOrEmpty(srcCntUomEntry) || srcCntUomEntry == "0")
                                return;

                            // Mapeo de unidad de recuento de inventario (OUOM.UomEntry/UomCode)
                            string? dstCntUomEntryStr = MasterDataMapper.MapByDescription(
                                src,
                                dst,
                                table: "OUOM",
                                codeField: "UomEntry",
                                descField: @"""UomCode""",
                                srcCode: srcCntUomEntry,
                                extensionWhereSQL: string.Empty,
                                out string? srcCntUomDesc);

                            if (dstCntUomEntryStr == null)
                            {
                                if (!string.IsNullOrEmpty(srcCntUomDesc))
                                {
                                    LogService.WriteLog(
                                        src,
                                        rule.Code,
                                        rule.Table,
                                        itmSrc.ItemCode,
                                        "WARNING",
                                        $"No se encontró mapeo para Unidad de recuento de inventario '{srcCntUomDesc}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.",
                                        "OITM.CntUnitMsr");
                                }
                                return;
                            }

                            if (int.TryParse(dstCntUomEntryStr, out int dstCntUomEntry))
                            {
                                itmDst.DefaultCountingUoMEntry = dstCntUomEntry;
                            }

                        }, "OITM.CntUnitMsr", rule);

                        // Método de valoración
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.CostAccountingMethod = itmSrc.CostAccountingMethod,
                            "OITM.EvalSystem",
                            rule);

                        // Niveles de stock globales (mínimo / máximo)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.MinInventory = itmSrc.MinInventory,
                            "OITM.MinLevel",
                            rule);

                        RuleHelpers.SetIfAllowed(
                            () => itmDst.MaxInventory = itmSrc.MaxInventory,
                            "OITM.MaxLevel",
                            rule);

                    }, "OITM.FLAP_INVENTARIO", rule);

                    // ================= PROPIEDADES =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Replicación de las64 propiedades de artículo (grupos de consulta QryGroup1..64)
                        for (int i =1; i <=64; i++)
                        {
                            string fieldName = $"QryGroup{i}";
                            RuleHelpers.SetIfAllowed(() => itmDst.Properties[i] = itmSrc.Properties[i], $"OITM.{fieldName}", rule);
                        }
                    }, "OITM.FLAP_PROPERTIES", rule);

                    // ================= DATOS DE PLANIFICACIONES =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Método de planificación (PlaningSys)
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            itmDst.PlanningSystem = itmSrc.PlanningSystem;
                        }, "OITM.PlaningSys", rule);

                        // Método de aprovisionamiento (PrcrmntMtd)
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            itmDst.ProcurementMethod = itmSrc.ProcurementMethod;
                        }, "OITM.PrcrmntMtd", rule);

                        // Intervalo de pedido (OrdrIntrvl) usando OCYC
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            string srcInterval = itmSrc.OrderIntervals.ToString();
                            if (string.IsNullOrWhiteSpace(srcInterval) || srcInterval == "0")
                                return;

                            // Mapeo de intervalo de pedido por descripción (OCYC.Name)
                            string? dstIntervalStr = MasterDataMapper.MapByDescription(
                                src,
                                dst,
                                table: "OCYC",
                                codeField: "Code",
                                descField: @"""Name""",
                                srcCode: srcInterval,
                                extensionWhereSQL: string.Empty,
                                out string? srcIntervalDesc);

                            if (dstIntervalStr == null)
                            {
                                if (!string.IsNullOrEmpty(srcIntervalDesc))
                                {
                                    LogService.WriteLog(
                                        src,
                                        rule.Code,
                                        rule.Table,
                                        itmSrc.ItemCode,
                                        "WARNING",
                                        $"No se encontró mapeo para Intervalo de pedido '{srcIntervalDesc}' (ItemCode: {itmSrc.ItemCode}). Se omite la asignación.",
                                        "OITM.OrdrIntrvl");
                                }
                                return;
                            }


                            itmDst.OrderIntervals = dstIntervalStr;

                        }, "OITM.OrdrIntrvl", rule);

                        // Pedido múltiple (OrdrMulti)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.OrderMultiple = itmSrc.OrderMultiple,
                            "OITM.OrdrMulti",
                            rule);

                        // Cantidad de pedido mínima (MinOrdrQty)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.MinOrderQuantity = itmSrc.MinOrderQuantity,
                            "OITM.MinOrdrQty",
                            rule);

                        // Regla de verificación ????

                        // Tiempo lead (LeadTime)
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.LeadTime = itmSrc.LeadTime,
                            "OITM.LeadTime",
                            rule);

                        // Días de tolerancia 
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.ToleranceDays = itmSrc.ToleranceDays,
                            "OITM.ToleranDay",
                            rule);

                    }, "OITM.FLAP_PLANNING", rule);

                    // ================= DATOS DE PRODUCCIÓN =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Artículo ficticio
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.IsPhantom = itmSrc.IsPhantom,
                            "OITM.Phantom",
                            rule);

                        // Método de emisión
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            itmDst.IssueMethod = itmSrc.IssueMethod;
                        }, "OITM.IssueMthd", rule);

                        // Costo estándar de producción
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.ProdStdCost = itmSrc.ProdStdCost,
                            "OITM.PrdStdCst",
                            rule);

                        // Incluir en implosión de costos estándar de producción
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.InCostRollup = itmSrc.InCostRollup,
                            "OITM.InCostRoll",
                            rule);

                    }, "OITM.FLAP_PRODUCTION", rule);

                    // ================= COMENTARIOS =================
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Comentarios de usuario del artículo
                        RuleHelpers.SetIfAllowed(
                            () => itmDst.User_Text = itmSrc.User_Text,
                            "OITM.UserText",
                            rule);

                    }, "OITM.FLAP_COMMENTS", rule);

                    // Ejecutar Add/Update del artículo en la compañía destino
                    int ret = exists ? itmDst.Update() : itmDst.Add();

                    LogService.HandleDiApiResult(src, dst, ret, rule.Code, "OITM", itemCode);

                    // Si la operación en destino fue exitosa:
                    // - avanzamos el checkpoint a la fecha/hora del registro actual
                    // - confirmamos la transacción en la base destino.
                    // Si falló, deshacemos la transacción.
                    if (ret ==0)
                    {
                        CheckpointService.UpdateFromRow(ref cp, rs, "UpdateDate", "UpdateTS");

                        if (dst.InTransaction)
                            dst.EndTransaction(BoWfTransOpt.wf_Commit);
                    }
                    else
                    {
                        if (dst.InTransaction)
                            dst.EndTransaction(BoWfTransOpt.wf_RollBack);
                    }


                    rs.MoveNext();
                }

                // Guardar checkpoint definitivo y liberar recursos COM
                CheckpointService.PersistCheckpoint(src, rule.Code, cp);

                System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(itmSrc);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(itmDst);
            }
            finally
            {
                // Asegurar desconexión de ambas compañías aunque ocurra una excepción
                factory.Disconnect(dst);
                factory.Disconnect(src);
            }
        }
    }
}