using Interface_ReplicarDatos.Replication.Models;
using Interface_ReplicarDatos.Replication.Services;
using Quartz.Util;
using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.CompilerServices.RuntimeHelpers;


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
                            SELECT 
                            ""CardCode"",
                            ""U_Replicate"",
                            ""UpdateDate"",
                            ""UpdateTS""
                            FROM ""OCRD""
                            WHERE (""UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}'
                            OR (""UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND ""UpdateTS"" > {cp.LastTime}))";

                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                rs.DoQuery(sql);

                if (!dst.InTransaction)
                    dst.StartTransaction();

                var bpSrc = (BusinessPartners)src.GetBusinessObject(BoObjectTypes.oBusinessPartners);

                var bpDst = (BusinessPartners)dst.GetBusinessObject(BoObjectTypes.oBusinessPartners);

                while (!rs.EoF)
                {
                    // Leer campos de la base mandatoria
                    string cardCode = rs.Fields.Item("CardCode").Value.ToString();
                    bpSrc.GetByKey(cardCode); // cargar BP origen si es necesario

                    // 1) Filtros lógicos según regla (tipo BP)
                    if (!PassesBpTypeFilter(rule, bpSrc.CardType))
                    {
                        rs.MoveNext();
                        continue;
                    }

                    // 2) Filtro por flag de replicación (U_Replicate o similar) según regla
                    if (rule.UseBPProperty && !string.IsNullOrWhiteSpace(rule.BPPropertyCode))
                    {
                        // Para simplificar, usamos U_Replicate (ya traído).
                        // Si BPPropertyCode fuera otra cosa, se puede extender aquí.
                        bool isReplicated = bpSrc.UserFields.Fields.Item("U_Replicate").Value == "Y";
                        if (rule.BPPropertyCode == "U_Replicate" && !isReplicated)
                        {
                            rs.MoveNext();
                            continue;
                        }
                    }


                    bool existsBp = bpDst.GetByKey(cardCode);

                    if (!existsBp)
                    {
                        bpDst.CardCode = bpSrc.CardCode;
                        bpDst.CardType = bpSrc.CardType;
                    }


                    // SETTERS DE DATOS

                    #region Grupo de Business partner
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? groupCode = MasterDataMapper.MapByDescription(src, dst, table: "OCRG", codeField: "GroupCode", descField: "GroupName", srcCode: bpSrc.GroupCode.ToString(), "", out string? srcGroupName);
                        if (groupCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcGroupName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Grupo de Socio de Negocios '{srcGroupName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.GroupCode = int.Parse(groupCode);

                    }, "OCRD.GroupCode", rule);
                    #endregion

                    #region Condicion de Pago de Business partner
                    RuleHelpers.SetIfAllowed(() =>
                        {
                            string? dstGroupNum = MasterDataMapper.MapByDescription(src, dst, table: "OCTG", codeField: "GroupNum", descField: "PymntGroup", srcCode: bpSrc.PayTermsGrpCode.ToString(), "", out string? srcPymntGroup);
                            if (dstGroupNum == null)
                            {
                                if (!string.IsNullOrEmpty(srcPymntGroup))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Condición de Pago '{srcPymntGroup}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                                }
                                return;
                            }
                            bpDst.PayTermsGrpCode = int.Parse(dstGroupNum);

                        }, "OCRD.GroupNum", rule);
                    #endregion

                    #region Tipo de envío
                    RuleHelpers.SetIfAllowed(() =>
                        {
                            string? dstShipType = MasterDataMapper.MapByDescription(src, dst, table: "OSHP", codeField: "TrnspCode", descField: "TrnspName", srcCode: bpSrc.ShippingType.ToString(), "", out string? srcShipType);
                            if (dstShipType == null)
                            {
                                if (!string.IsNullOrEmpty(bpSrc.ShippingType.ToString()))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Tipo de Envío '{srcShipType}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                                }
                                return;
                            }
                            bpDst.ShippingType = int.Parse(dstShipType);
                        }, "OCRD.ShipType", rule);
                    #endregion

                    #region Indicador
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstIndicator = MasterDataMapper.MapByDescription(src, dst, table: "OIDC", codeField: "Code", descField: "Name", srcCode: bpSrc.Indicator, "", out string? srcIndicator);
                        if (dstIndicator == null)
                        {
                            if (!string.IsNullOrEmpty(srcIndicator))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Indicador '{srcIndicator}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.Indicator = dstIndicator;

                    }, "OCRD.Indicator", rule);
                    #endregion

                    #region Proyecto de socio de negocios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstProjectCode = MasterDataMapper.MapByDescription(src, dst, table: "OPRJ", codeField: "PrjCode", descField: "PrjName", srcCode: bpSrc.ProjectCode, "", out string? srcProjectName);
                        if (dstProjectCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcProjectName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Proyecto de Socio de Negocios '{srcProjectName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.ProjectCode = dstProjectCode;
                    }, "OCRD.ProjectCod", rule);
                    #endregion

                    #region Industria de socio de negocios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstIndustryCode = MasterDataMapper.MapByDescription(src, dst, table: "OOND", codeField: "IndCode", descField: "IndName", srcCode: bpSrc.Industry.ToString(), "", out string? srcIndustryName);
                        if (dstIndustryCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcIndustryName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Industria de Socio de Negocios '{srcIndustryName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.Industry = int.Parse(dstIndustryCode);
                    }, "OCRD.IndustryC", rule);
                    #endregion

                    #region Persona de contacto predeterminada
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstContactPerson = MasterDataMapper.MapByDescription(src, dst, table: "OCPR", codeField: "Name", descField: "Name", srcCode: bpSrc.ContactPerson, extensionWhereSQL: @$"""CardCode"" = '{bpSrc.CardCode}'", out string? srcContactPerson);
                        if (dstContactPerson == null)
                        {
                            if (!string.IsNullOrEmpty(srcContactPerson))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Persona de Contacto '{srcContactPerson}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.ContactPerson = dstContactPerson;

                    }, "OCRD.CntctPrsn", rule);
                    #endregion

                    #region Encargado de venta/compra
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstSlpCode = MasterDataMapper.MapByDescription(src, dst, table: "OSLP", codeField: "SlpCode", descField: "SlpName", srcCode: bpSrc.SalesPersonCode.ToString(), "", out string? srcSlpName);
                        if (dstSlpCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcSlpName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Encargado de Venta/Compra '{srcSlpName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.SalesPersonCode = int.Parse(dstSlpCode);

                    }, "OCRD.SlpCode", rule);
                    #endregion

                    #region Territorio de socio de negocios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstTerritoryID = MasterDataMapper.MapByDescription(src, dst, table: "OTER", codeField: "territryID", descField: "descript", srcCode: bpSrc.Territory.ToString(), "", out string? srcTerritoryName);
                        if (dstTerritoryID == null)
                        {
                            if (!string.IsNullOrEmpty(srcTerritoryName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Territorio de Socio de Negocios '{srcTerritoryName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.Territory = int.Parse(dstTerritoryID);

                    }, "OCRD.Territory", rule);
                    #endregion

                    #region Lenguaje de socio de negocios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstLangCode = MasterDataMapper.MapByDescription(src, dst, table: "OLNG", codeField: "Code", descField: "Name", srcCode: bpSrc.LanguageCode.ToString(), "", out string? srcLangName);
                        if (dstLangCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcLangName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Lenguaje de Socio de Negocios '{srcLangName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.LanguageCode = int.Parse(dstLangCode);

                    }, "OCRD.LngCode", rule);
                    #endregion

                    #region OTROS CAMPOS
                    RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_FiscIdType").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_FiscIdType").Value, "OCRD.U_B1SYS_FiscIdType", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_VATCtg").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_VATCtg").Value, "OCRD.U_B1SYS_VATCtg", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.VatGroup = bpSrc.VatGroup, "OCRD.VatGroup", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.CardName = bpSrc.CardName, "OCRD.CardName", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.CardForeignName = bpSrc.CardForeignName, "OCRD.CardFName", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FederalTaxID = bpSrc.FederalTaxID, "OCRD.LicTradNum", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Currency = bpSrc.Currency, "OCRD.Currency", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Phone1 = bpSrc.Phone1, "OCRD.Phone1", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Phone2 = bpSrc.Phone2, "OCRD.Phone2", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Cellular = bpSrc.Cellular, "OCRD.Cellular", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Fax = bpSrc.Fax, "OCRD.Fax", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.EmailAddress = bpSrc.EmailAddress, "OCRD.E_Mail", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Website = bpSrc.Website, "OCRD.IntrntSite", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Password = bpSrc.Password, "OCRD.Password", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.CompanyPrivate = bpSrc.CompanyPrivate, "OCRD.CmpPrivate", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.AliasName = bpSrc.AliasName, "OCRD.AliasName", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Valid = bpSrc.Valid, "OCRD.validFor", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.ValidFrom = bpSrc.ValidFrom, "OCRD.validFrom", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.ValidTo = bpSrc.ValidTo, "OCRD.validTo", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.ValidRemarks = bpSrc.ValidRemarks, "OCRD.ValidComm", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Frozen = bpSrc.Frozen, "OCRD.frozenFor", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FrozenFrom = bpSrc.FrozenFrom, "OCRD.frozenFrom", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FrozenTo = bpSrc.FrozenTo, "OCRD.frozenTo", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FrozenRemarks = bpSrc.FrozenRemarks, "OCRD.FrozenComm", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.AdditionalID = bpSrc.AdditionalID, "OCRD.AddID", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.VatIDNum = bpSrc.VatIDNum, "OCRD.VatIdUnCmp", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.Notes = bpSrc.Notes, "OCRD.Notes", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.GlobalLocationNumber = bpSrc.GlobalLocationNumber, "OCRD.GlblLocNum", rule);
                    #endregion


                    // Add / Update
                    int ret = existsBp ? bpDst.Update() : bpDst.Add();

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
                System.Runtime.InteropServices.Marshal.ReleaseComObject(bpSrc);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(bpDst);
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

        private static bool PassesBpTypeFilter(RepRule rule, BoCardTypes cardType)
        {
            // cardType: 'C' clientes, 'S' proveedores
            switch (rule.RepBPType)
            {
                case "P": // solo proveedores
                    return cardType == BoCardTypes.cSupplier;
                case "C": // solo clientes
                    return cardType == BoCardTypes.cCustomer;
                case "B": // ambos
                default:
                    return cardType == BoCardTypes.cCustomer || cardType == BoCardTypes.cSupplier;
            }
        }
    }
}

