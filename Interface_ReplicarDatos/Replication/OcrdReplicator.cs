using Interface_ReplicarDatos.Replication.Models;
using Interface_ReplicarDatos.Replication.Services;
using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Interface_ReplicarDatos.Replication
{
    public static class OcrdReplicator
    {
        /// <summary>
        /// Ejecuta la regla de replicación OCRD: lee desde SrcDB (PHXA) y escribe en DstDB.
        /// Usa Checkpoints, FieldMapping, ExcludeCSV, AssignJSON, U_Replicate, etc.
        /// </summary>
        public static void Run(RepRule rule, IDiApiConnectionFactory factory)
        {
            Company src = null;
            Company dst = null;

            try
            {
                src = factory.Connect(rule.SrcDB); // PHXA (PMX_TEST)
                dst = factory.Connect(rule.DstDB); // PHXB / MUNDOBB / ML

                // Cargar checkpoint (se guarda por regla + base destino)
                var cp = CheckpointService.LoadCheckpoint(src, rule.Code);

                // Recordset origen
                var rs = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);
                string sql = $@"
                            SELECT ""CardCode"",""CardName"",""CardType"",""GroupCode"",""GroupNum"",
                                   ""Phone1"",""Phone2"",""E_Mail"",""VatGroup"",""frozenFor"",
                                   IFNULL(""U_Replicate"",'N') AS ""U_Replicate"",
                                   ""UpdateDate"",""UpdateTS""
                            FROM ""OCRD""
                            WHERE 
                             (""UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}'
                              OR (""UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND ""UpdateTS"" > {cp.LastTime}))";

                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                rs.DoQuery(sql);

                if (!dst.InTransaction)
                    dst.StartTransaction();

                var bp = (BusinessPartners)dst.GetBusinessObject(BoObjectTypes.oBusinessPartners);

                while (!rs.EoF)
                {
                    string cardCode = rs.Fields.Item("CardCode").Value.ToString();
                    string cardName = rs.Fields.Item("CardName").Value.ToString();
                    string cardType = rs.Fields.Item("CardType").Value.ToString(); // 'C' / 'S'
                    string phone1 = rs.Fields.Item("Phone1").Value.ToString();
                    string phone2 = rs.Fields.Item("Phone2").Value.ToString();
                    string email = rs.Fields.Item("E_Mail").Value.ToString();
                    string vatGroup = rs.Fields.Item("VatGroup").Value.ToString();
                    string groupCode = rs.Fields.Item("GroupCode").Value.ToString();
                    string frozen = rs.Fields.Item("frozenFor").Value.ToString();
                    string uRep = rs.Fields.Item("U_Replicate").Value.ToString();

                    // 1) Filtros lógicos según regla (tipo BP)
                    if (!PassesBpTypeFilter(rule, cardType))
                    {
                        rs.MoveNext();
                        continue;
                    }

                    // 2) Filtro por flag de replicación (U_Replicate o similar) según regla
                    if (rule.UseBPProperty && !string.IsNullOrWhiteSpace(rule.BPPropertyCode))
                    {
                        // Para simplificar, usamos U_Replicate (ya traído).
                        // Si BPPropertyCode fuera otra cosa, se puede extender aquí.
                        if (rule.BPPropertyCode == "U_Replicate" && uRep != "Y")
                        {
                            rs.MoveNext();
                            continue;
                        }
                    }
                    

                    bool exists = bp.GetByKey(cardCode);

                    if (!exists)
                    {
                        bp.CardCode = cardCode;
                        bp.CardType = cardType == "S"
                            ? BoCardTypes.cSupplier
                            : BoCardTypes.cCustomer;
                    }

                    RuleHelpers.SetIfAllowed(() => bp.CardName = cardName, "OCRD.CardName", rule);
                    RuleHelpers.SetIfAllowed(() => bp.Phone1 = phone1, "OCRD.Phone1", rule);
                    RuleHelpers.SetIfAllowed(() => bp.Phone2 = phone2, "OCRD.Phone2", rule);
                    RuleHelpers.SetIfAllowed(() => bp.EmailAddress = email, "OCRD.E_Mail", rule);
                    RuleHelpers.SetIfAllowed(() => bp.Frozen = (frozen == "Y") ? BoYesNoEnum.tYES : BoYesNoEnum.tNO, "OCRD.frozenFor", rule); // Congelado (FrozenFor)

                    #region SETEAR VALORES SEGUN MAPPING DESDE TABLA "GNA_REP_FMAP"

                        vatGroup = FieldMappingService.Apply(rule.SrcDB, rule.DstDB, "OCRD", "VatGroup", vatGroup);
                        RuleHelpers.SetIfAllowed(() => bp.VatGroup = vatGroup, "OCRD.VatGroup", rule);  // Condición de IVA

                        groupCode = FieldMappingService.Apply(rule.SrcDB, rule.DstDB, "OCRD", "GroupCode", groupCode);
                        RuleHelpers.SetIfAllowed(() => bp.GroupCode = int.Parse(groupCode), "OCRD.GroupCode", rule);  // Grupo de Business partner

                    #endregion

                    // Add / Update
                    int ret = exists ? bp.Update() : bp.Add();
                    
                    LogService.HandleDiApiResult(src, dst, ret, rule.Code, "OCRD", cardCode);

                    // 5) Actualizar checkpoint con la fila actual
                    CheckpointService.UpdateFromRow(ref cp, rs, "UpdateDate", "UpdateTS");

                    rs.MoveNext();
                }

                if (dst.InTransaction)
                    dst.EndTransaction(BoWfTransOpt.wf_Commit);

                // Guardar checkpoint final
                
                CheckpointService.PersistCheckpoint(src, rule.Code, cp);

                System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(bp);
            }
            finally
            {
                factory.Disconnect(dst);
                factory.Disconnect(src);
            }
        }

        private static bool PassesBpTypeFilter(RepRule rule, string cardType)
        {
            // cardType: 'C' clientes, 'S' proveedores
            switch (rule.RepBPType)
            {
                case "P": // solo proveedores
                    return cardType == "S";
                case "C": // solo clientes
                    return cardType == "C";
                case "B": // ambos
                default:
                    return cardType == "C" || cardType == "S";
            }
        }
    }
}

