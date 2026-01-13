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
            Company src = null; // Compañía origen (PHXA)
            Company dst = null; // Compañía destino (PHXB, etc.)

            try
            {
                // Conexión a las compañías usando la fábrica de conexiones DI API
                src = factory.Connect(rule.SrcDB);
                dst = factory.Connect(rule.DstDB);

                // Cargar checkpoint para saber desde qué fecha/hora continuar la replicación
                var cp = CheckpointService.LoadCheckpoint(src, rule.Code);

                // Recordset para obtener los artículos cuyo precio fue modificado desde el último checkpoint
                var rsItems = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);

                string sql = $@"SELECT 
                            ""ItemCode"",
                            IFNULL(""U_Replicate"", 'N') AS ""U_Replicate"",
                            ""UpdateDate"",
                            ""UpdateTS"" 
                            FROM ""OITM"" 
                            WHERE (""UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}' 
                            OR (""UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND ""UpdateTS"" > {cp.LastTime})) AND ""ItemCode"" = 'NEW1'";

                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                rsItems.DoQuery(sql);

                // Objetos Items de origen y destino (para leer y escribir listas de precios)
                var itmSrc = (Items)src.GetBusinessObject(BoObjectTypes.oItems);
                var itmDst = (Items)dst.GetBusinessObject(BoObjectTypes.oItems);

                // Recorrer cada artículo con cambios de precio
                while (!rsItems.EoF)
                {
                    string itemCode = rsItems.Fields.Item("ItemCode").Value.ToString();


                    // Si el artículo no existe en destino, se omite (no se crean artículos aquí)
                    if (!itmDst.GetByKey(itemCode))
                    {
                        rsItems.MoveNext();
                        continue;
                    }

                    // Cargar artículo de origen para leer sus listas de precios (ITM1)
                    itmSrc.GetByKey(itemCode);

                    // Cantidad de listas de precios definidas en el artículo origen
                    int priceListCount = itmSrc.PriceList.Count;

                    // Iniciar transacción en destino si aún no hay una abierta
                    if (!dst.InTransaction)
                        dst.StartTransaction();

                    // Bloque de asignación sujeto a reglas de exclusión/permiso (RuleHelpers)
                    bool hasChanges = false;
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Recorrer todas las listas de precios del artículo origen
                        for (int i = 0; i < priceListCount; i++)
                        {
                            itmSrc.PriceList.SetCurrentLine(i);
                            int srcListNum = itmSrc.PriceList.PriceList;   // ITM1.PriceList (número de lista origen)
                            double srcPrice = itmSrc.PriceList.Price;      // ITM1.Price (precio en esa lista)

                            // Saltear listas sin precio definido (0)
                            if (srcPrice == 0)
                                continue;

                            // Consultar en OPLN si la lista de precios está marcada para replicación (U_Replicate = 'Y')
                            string srcListNumStr = srcListNum.ToString();

                            var rsOpln = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);
                            string q = $@"SELECT IFNULL(""U_Replicate"", 'N') FROM ""OPLN"" WHERE ""ListNum"" = {srcListNum}";
                            rsOpln.DoQuery(q);
                            if (!rsOpln.EoF)
                            {
                                string replicateFlag = rsOpln.Fields.Item(0).Value.ToString();
                                if (replicateFlag != "Y")
                                {
                                    // La lista de precios no está marcada para replicación → se omite
                                    rsOpln.MoveNext();
                                    continue;
                                }
                            }

                            // Mapear el número de lista de precios de origen a destino (OPLN)
                            string? dstListNumStr = MasterDataMapper.MapByDescription(
                                src,
                                dst,
                                table: "OPLN",            // Tabla de listas de precios
                                codeField: "ListNum",     // Código numérico de lista
                                descField: @"""ListName""",
                                srcCode: srcListNumStr,
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

                            //// Convertir el código de lista de destino a int
                            //if (!int.TryParse(dstListNumStr, out int dstListNum))
                            //    continue;

                            // Buscar en el Items destino la línea de ITM1 que corresponde a esa lista de precios
                            for (int j = 0; j < itmDst.PriceList.Count; j++)
                            {
                                itmDst.PriceList.SetCurrentLine(j);
                                if (itmDst.PriceList.PriceList == Convert.ToInt32(dstListNumStr))
                                {
                                    double dstPrice = itmDst.PriceList.Price;
                                    if (Math.Abs(dstPrice - srcPrice) < 0.0000001)
                                        break;

                                    RuleHelpers.SetIfAllowed(
                                        () => itmDst.PriceList.Price = srcPrice,
                                        "ITM1.Price",
                                        rule);

                                    hasChanges = true;
                                    break;
                                }
                            }

                        }
                    }, "ITM1.FLAP_PRICELISTS", rule); // Lógica agrupada bajo un nombre lógico de "pestaña" de listas de precios

                    // Solo llamar a Update si hubo algún cambio real
                    int ret = 0;
                    if (hasChanges)
                    {
                        ret = itmDst.Update();
                        LogService.HandleDiApiResult(src, dst, ret, rule.Code, "ITM1", itemCode);
                    }
                    else
                    {
                        // Sin cambios: no llamamos a Update, pero tampoco es error
                        LogService.WriteLog(src, rule.Code, "ITM1", itemCode, "INFO",
                            "Sin cambios de precio detectados, se omite Update.", "ITM1.Price");
                    }

                    // Actualizar checkpoint con la fecha/hora del artículo procesado
                    if (ret == 0)
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