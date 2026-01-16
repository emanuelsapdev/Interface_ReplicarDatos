using Interface_ReplicarDatos.Replication.Models;
using Interface_ReplicarDatos.Replication.Services;
using SAPbobsCOM;
using System;

namespace Interface_ReplicarDatos.Replication
{
    public static class OitmPriceListReplicator
    {
        public static void Run(RepRule rule, IDiApiConnectionFactory factory)
        {
            Company src = null; // Compañía origen (ej.: PHXA)
            Company dst = null; // Compañía destino (ej.: PHXB)

            try
            {
                // Conexión a las compañías usando la fábrica de conexiones DI API
                src = factory.Connect(rule.SrcDB);
                dst = factory.Connect(rule.DstDB);

                // Cargar checkpoint para saber desde qué fecha/hora continuar la replicación
                var cp = CheckpointService.LoadCheckpoint(src, rule.Code);

                // Recordset para obtener los artículos/listas cuyo precio fue modificado desde el último checkpoint
                var rsItems = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);

                // Query base: partir de OPLN (listas de precios) -> ITM1 (precios) -> OITM (artículos)
                // Se usa la "fecha/hora de actualización" personalizada en ITM1 (U_UpdateDate / U_UpdateTS)
                // para detectar qué líneas de precio cambiaron desde el último checkpoint.
                string sql = $@"
                        SELECT DISTINCT
                            OITM.""ItemCode"",
                            OITM.""UpdateDate"",
                            OITM.""UpdateTS""
                        FROM ""OPLN"" OPLN
                        INNER JOIN ""ITM1"" ITM1 ON ITM1.""PriceList"" = OPLN.""ListNum""
                        INNER JOIN ""OITM"" OITM ON OITM.""ItemCode"" = ITM1.""ItemCode""
                        WHERE
                            (ITM1.""U_UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}'
                             OR (ITM1.""U_UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND ITM1.""U_UpdateTS"" > {cp.LastTime}))
                        AND IFNULL(OPLN.""{rule.RepPropertyCode}"", 'Y') = 'Y'";

                // Si la regla usa propiedad de replicación y el flag es U_Replicate (u otro),
                // también filtramos las líneas de ITM1 por ese UDF (por ejemplo ITM1.U_Replicate = 'Y').
                if (rule.UseRepProperty && !string.IsNullOrWhiteSpace(rule.RepPropertyCode))
                {
                    sql += @$" AND IFNULL(ITM1.""{rule.RepPropertyCode}"", 'Y') = 'Y'";
                }

                // Filtro adicional configurable (Rule.FilterSQL), por ejemplo filtrar por grupos de artículos, etc.
                string filterSql = string.Empty;
                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                // Ejecutar la consulta en la compañía origen
                rsItems.DoQuery(sql);

                // Objetos Items de origen y destino (para leer y escribir listas de precios)
                var itmSrc = (Items)src.GetBusinessObject(BoObjectTypes.oItems);
                var itmDst = (Items)dst.GetBusinessObject(BoObjectTypes.oItems);

                // Recorrer cada artículo con al menos una línea de ITM1 que cumple las condiciones anteriores
                while (!rsItems.EoF)
                {
                    string itemCode = rsItems.Fields.Item("ItemCode").Value.ToString();

                    // Si el artículo no existe en destino, se omite (no se crean artículos aquí)
                    if (!itmDst.GetByKey(itemCode))
                    {
                        rsItems.MoveNext();
                        continue;
                    }

                    // Cargar artículo de origen para leer sus listas de precios (colección ITM1)
                    itmSrc.GetByKey(itemCode);

                    // Cantidad de listas de precios definidas en el artículo origen
                    int priceListCount = itmSrc.PriceList.Count;

                    // Iniciar transacción en destino si aún no hay una abierta
                    if (!dst.InTransaction)
                        dst.StartTransaction();

                    // Bloque de asignación sujeto a reglas de exclusión/permiso (RuleHelpers)
                    bool hasChanges = false; // indica si hubo cambios reales en alguna lista de precios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Recordset auxiliar para consultar datos de OPLN dentro del bucle de listas
                        var rec = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);

                        // Recorrer todas las listas de precios del artículo origen
                        for (int i =0; i < priceListCount; i++)
                        {
                            itmSrc.PriceList.SetCurrentLine(i);
                            int srcListNum = itmSrc.PriceList.PriceList; // ITM1.PriceList (número de lista origen)
                            double dstPrice = itmDst.PriceList.Price;

                            // Leer el flag de replicación en ITM1 (U_Replicate u otro definido en la regla)
                            string valueRepProp = itmSrc.PriceList.UserFields.Fields.Item(rule.RepPropertyCode).Value;
                            if (valueRepProp == "N") continue; // si la línea no está marcada para replicar, se omite

                            // Leer también el flag de replicación a nivel de lista de precios (OPLN)
                            rec.DoQuery($@"SELECT IFNULL(OPLN.""{rule.RepPropertyCode}"", 'Y') FROM OPLN WHERE OPLN.""ListNum"" = {srcListNum}");
                            string repPropList = rec.Fields.Item(0).Value;
                            if (repPropList == "N") continue; // si la lista no está marcada, se omite

                            // Mapear el número de lista de precios de origen a destino (OPLN)
                            string? dstListNumStr = MasterDataMapper.MapByDescription(
                                src,
                                dst,
                                table: "OPLN", // Tabla de listas de precios
                                codeField: "ListNum", // Código numérico de lista
                                descField: @"""ListName""",
                                srcCode: srcListNum.ToString(),
                                extensionWhereSQL: string.Empty,
                                out string? srcListName);

                            // Si no hay mapeo configurado, se registra un WARNING y se omite esa lista
                            if (dstListNumStr == null)
                            {
                                if (!string.IsNullOrEmpty(srcListName))
                                {
                                    LogService.WriteLog(
                                        src,
                                        rule.Code,
                                        rule.Table,
                                        itemCode,
                                        "WARNING",
                                        $"No se encontró mapeo para Lista de precios '{srcListName}' (ListNum {srcListNum}) en artículo {itemCode}. Se omite la asignación.",
                                        "ITM1.PriceList");
                                }
                                continue;
                            }

                            // Buscar en el Items destino la línea de ITM1 que corresponde a esa lista de precios
                            for (int j = 0; j < itmDst.PriceList.Count; j++)
                            {
                                itmDst.PriceList.SetCurrentLine(j);
                                if (itmDst.PriceList.PriceList == Convert.ToInt32(dstListNumStr))
                                {
                                    // Precio origen/destino para esta lista
                                    double srcPrice = itmSrc.PriceList.Price; // ITM1.Price (precio en esa lista)
                                    

                                    // Si el precio es igual (con tolerancia), no se hace nada
                                    if (Math.Abs(dstPrice - srcPrice) <0.0000001)
                                        break;

                                    // Actualizar precio en destino respetando reglas de replicación
                                    RuleHelpers.SetIfAllowed(
                                        () => itmDst.PriceList.Price = srcPrice,
                                        "ITM1.Price",
                                        rule);

                                    double srcFactor = itmSrc.PriceList.Factor; // ITM1.Factor (factor en esa lista)
                                    RuleHelpers.SetIfAllowed(
                                    () => itmDst.PriceList.Factor = srcFactor,
                                    "ITM1.Factor",
                                    rule);

                                    RuleHelpers.SetIfAllowed(() =>  // Mapeo de Lista de Precios Base
                                    {
                                        var srcBasePrice = itmSrc.PriceList.BasePriceList;
                                        if (srcBasePrice == 0)
                                            return;

                                        string? dstBaseLPrice = MasterDataMapper.MapByDescription(src, dst, table: "OPLN", codeField: "ListNum", descField: @"""ListName""", srcCode: srcBasePrice.ToString(), extensionWhereSQL: string.Empty, out string? dstBaseLPriceName);
                                        if (dstBaseLPrice == null)
                                        {
                                            if (!string.IsNullOrEmpty(dstBaseLPriceName))
                                            {
                                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: $"item: {itemCode} | list: {srcListName}" , "WARNING", $"No se encontró mapeo para Lista de Precios Base '{dstBaseLPriceName}' (ItemCode: {itmSrc.ItemCode} / Lista de Precios: {srcListName}). Se omite la asignación.", "ITM1.BasePLNum");
                                            }
                                            return;
                                        }
                                        itmDst.PriceList.BasePriceList = int.Parse(dstBaseLPrice);
                                    }, "ITM1.BasePLNum", rule);

                                    hasChanges = true;
                                    break;
                                }
                            }

                        }
                    }, "ITM1.FLAP_PRICELISTS", rule); // "Pestaña" lógica de listas de precios para el motor de reglas

                    // Solo llamar a Update si hubo algún cambio real en las listas de precios
                    int ret =0;
                    if (hasChanges)
                    {
                        ret = itmDst.Update();
                        LogService.HandleDiApiResult(src, dst, ret, rule.Code, "ITM1", itemCode);
                    }
                    else
                    {
                        // Sin cambios: no llamamos a Update, pero lo dejamos registrado como INFO
                        LogService.WriteLog(src, rule.Code, "ITM1", itemCode, "INFO",
                            "Sin cambios de precio detectados, se omite Update.", "ITM1.Price");
                    }

                    // Actualizar checkpoint con la fecha/hora del artículo procesado
                    if (ret ==0)
                    {
                        CheckpointService.UpdateFromRow(ref cp, rsItems, "UpdateDate", "UpdateTS");

                        // Confirmar la transacción si todo fue OK
                        if (dst.InTransaction)
                            dst.EndTransaction(BoWfTransOpt.wf_Commit);
                    }
                    else
                    {
                        // Revertir la transacción si hubo error en el Update
                        if (dst.InTransaction)
                            dst.EndTransaction(BoWfTransOpt.wf_RollBack);
                    }

                    // Pasar al siguiente artículo
                    rsItems.MoveNext();
                }


                // Persistir el checkpoint actualizado para futuras ejecuciones
                CheckpointService.PersistCheckpoint(src, rule.Code, cp);

                // Liberar objetos COM
                System.Runtime.InteropServices.Marshal.ReleaseComObject(rsItems);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(itmSrc);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(itmDst);
            }
            finally
            {
                // Desconectar de las compañías, aunque haya ocurrido una excepción
                factory.Disconnect(dst);
                factory.Disconnect(src);
            }
        }
    }
}