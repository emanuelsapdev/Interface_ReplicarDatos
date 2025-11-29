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

                    #region SETTERS DE DATOS - CABECERA

                    // Nombre del socio de negocios
                    RuleHelpers.SetIfAllowed(() => bpDst.CardName = bpSrc.CardName, "OCRD.CardName", rule);

                    // Nombre Extranjero del socio de negocios
                    RuleHelpers.SetIfAllowed(() => bpDst.CardForeignName = bpSrc.CardForeignName, "OCRD.CardFName", rule);

                    // Grupo de Cliente / Proveedor
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstGroupCode = MasterDataMapper.MapByDescription(src, dst, table: "OCRG", codeField: "GroupCode", descField: "GroupName", srcCode: bpSrc.GroupCode.ToString(), "", out string? srcGroupName);
                        if (dstGroupCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcGroupName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Grupo de Socio de Negocios '{srcGroupName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.GroupCode = int.Parse(dstGroupCode);

                    }, "OCRD.GroupCode", rule);

                    // Moneda
                    RuleHelpers.SetIfAllowed(() => bpDst.Currency = bpSrc.Currency, "OCRD.Currency", rule);

                    // Tipo Indicador fiscal
                    RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_FiscIdType").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_FiscIdType").Value, "OCRD.U_B1SYS_FiscIdType", rule);

                    // Identificación fiscal
                    RuleHelpers.SetIfAllowed(() => bpDst.FederalTaxID = bpSrc.FederalTaxID, "OCRD.LicTradNum", rule);


                    #endregion


                    #region SETTERS DE DATOS - SOLAPA GENERAL

                    // Teléfono 1
                    RuleHelpers.SetIfAllowed(() => bpDst.Phone1 = bpSrc.Phone1, "OCRD.Phone1", rule); 
                    
                    // Teléfono 2
                    RuleHelpers.SetIfAllowed(() => bpDst.Phone2 = bpSrc.Phone2, "OCRD.Phone2", rule); 
                    
                    // Celular
                    RuleHelpers.SetIfAllowed(() => bpDst.Cellular = bpSrc.Cellular, "OCRD.Cellular", rule); 
                    
                    // Fax
                    RuleHelpers.SetIfAllowed(() => bpDst.Fax = bpSrc.Fax, "OCRD.Fax", rule);    
                    
                    // Correo electrónico
                    RuleHelpers.SetIfAllowed(() => bpDst.EmailAddress = bpSrc.EmailAddress, "OCRD.E_Mail", rule); 
                    
                    // Sitio web
                    RuleHelpers.SetIfAllowed(() => bpDst.Website = bpSrc.Website, "OCRD.IntrntSite", rule); 

                    // Forma de envío
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstShipType = MasterDataMapper.MapByDescription(src, dst, table: "OSHP", codeField: "TrnspCode", descField: @"""TrnspName""", srcCode: bpSrc.ShippingType.ToString(), "", out string? srcShipType);
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

                    // Indicador de factoring
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstIndicator = MasterDataMapper.MapByDescription(src, dst, table: "OIDC", codeField: "Code", descField: @"""Name""", srcCode: bpSrc.Indicator, "", out string? srcIndicator);
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

                    // Clave de acceso
                    RuleHelpers.SetIfAllowed(() => bpDst.Password = bpSrc.Password, "OCRD.Password", rule);

                    // Proyecto de socio de negocios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstProjectCode = MasterDataMapper.MapByDescription(src, dst, table: "OPRJ", codeField: "PrjCode", descField: @"""PrjName""", srcCode: bpSrc.ProjectCode, "", out string? srcProjectName);
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

                    // Industria
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstIndustryCode = MasterDataMapper.MapByDescription(src, dst, table: "OOND", codeField: "IndCode", descField: @"""IndName""", srcCode: bpSrc.Industry.ToString(), "", out string? srcIndustryName);
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

                    // Tipo de operación comercial
                    RuleHelpers.SetIfAllowed(() => bpDst.CompanyPrivate = bpSrc.CompanyPrivate, "OCRD.CmpPrivate", rule);

                    // Nombre alias
                    RuleHelpers.SetIfAllowed(() => bpDst.AliasName = bpSrc.AliasName, "OCRD.AliasName", rule);

                    // Persona de contacto predeterminada
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstContactPerson = MasterDataMapper.MapByDescription(src, dst, table: "OCPR", codeField: "Name", descField: @"""Name""", srcCode: bpSrc.ContactPerson, extensionWhereSQL: @$"""CardCode"" = '{bpSrc.CardCode}'", out string? srcContactPerson);
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

                    // ID número 2
                    RuleHelpers.SetIfAllowed(() => bpDst.AdditionalID = bpSrc.AdditionalID, "OCRD.AddID", rule);

                    // ID fiscal federal unificado
                    RuleHelpers.SetIfAllowed(() => bpDst.VatIDNum = bpSrc.VatIDNum, "OCRD.VatIdUnCmp", rule);

                    // Comentarios
                    RuleHelpers.SetIfAllowed(() => bpDst.Notes = bpSrc.Notes, "OCRD.Notes", rule);

                    // Empleado del dpto.de ventas
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstSlpCode = MasterDataMapper.MapByDescription(src, dst, table: "OSLP", codeField: "SlpCode", descField: @"""SlpName""", srcCode: bpSrc.SalesPersonCode.ToString(), "", out string? srcSlpName);
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

                    // Responsable
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstAgentCode = MasterDataMapper.MapByDescription(src, dst, table: "OAGP", codeField: "AgentCode", descField: @"""AgentName""", srcCode: bpSrc.AgentCode.ToString(), "", out string? srcAgentName);

                        if (dstAgentCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcAgentName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Agente '{srcAgentName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }

                        bpDst.AgentCode = dstAgentCode;

                    }, "OCRD.AgentCode", rule);

                    // Código canal SN
                    RuleHelpers.SetIfAllowed(() => bpDst.ChannelBP = bpSrc.ChannelBP, "OCRD.ChannlBP", rule);

                    // Técnico
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstTechnicalCode = MasterDataMapper.MapByDescription(src, dst, table: "OHEM", codeField: "empID", descField: @"""firstName"" || ""lastName""", srcCode: bpSrc.DefaultTechnician.ToString(), "", out string? srcTechnicalName);
                        
                        if (dstTechnicalCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcTechnicalName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Técnico '{srcTechnicalName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }

                        bpDst.DefaultTechnician = int.Parse(dstTechnicalCode);

                    }, "OCRD.DfTcnician", rule);

                    // Territorio
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstTerritoryID = MasterDataMapper.MapByDescription(src, dst, table: "OTER", codeField: "territryID", descField: @"""descript""", srcCode: bpSrc.Territory.ToString(), "", out string? srcTerritoryName);
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

                    // Idioma
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstLangCode = MasterDataMapper.MapByDescription(src, dst, table: "OLNG", codeField: "Code", descField: @"""Name""", srcCode: bpSrc.LanguageCode.ToString(), "", out string? srcLangName);
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

                    // GLN
                    RuleHelpers.SetIfAllowed(() => bpDst.GlobalLocationNumber = bpSrc.GlobalLocationNumber, "OCRD.GlblLocNum", rule);

                    // Validez, Fechas y Comentarios de validez
                    RuleHelpers.SetIfAllowed(() => bpDst.Valid = bpSrc.Valid, "OCRD.validFor", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.ValidFrom = bpSrc.ValidFrom, "OCRD.validFrom", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.ValidTo = bpSrc.ValidTo, "OCRD.validTo", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.ValidRemarks = bpSrc.ValidRemarks, "OCRD.ValidComm", rule);

                    // Congelamiento, Fechas y Comentarios de congelamiento
                    RuleHelpers.SetIfAllowed(() => bpDst.Frozen = bpSrc.Frozen, "OCRD.frozenFor", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FrozenFrom = bpSrc.FrozenFrom, "OCRD.frozenFrom", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FrozenTo = bpSrc.FrozenTo, "OCRD.frozenTo", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.FrozenRemarks = bpSrc.FrozenRemarks, "OCRD.FrozenComm", rule);

                    #endregion

                    #region SETTERS DE DATOS - SOLAPA PERSONAS DE CONTACTO ??????????????????

                    #endregion

                    #region SETTERS DE DATOS - SOLAPA DIRECCIONES ??????????????????

                    #endregion

                    #region  SETTERS DE DATOS - SOLAPA CONDICIONES DE PAGO

                    // Condición de Pago
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstGroupNum = MasterDataMapper.MapByDescription(src, dst, table: "OCTG", codeField: "GroupNum", descField: @"""PymntGroup""", srcCode: bpSrc.PayTermsGrpCode.ToString(), "", out string? srcPymntGroup);
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

                    // % intereses por retraso
                    RuleHelpers.SetIfAllowed(() => bpDst.IntrestRatePercent = bpSrc.IntrestRatePercent, "OCRD.IntrstRate", rule);

                    // Lista de precios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstListNum = MasterDataMapper.MapByDescription(src, dst, table: "OPLN", codeField: "ListNum", descField: @"""ListName""", srcCode: bpSrc.PriceListNum.ToString(), "", out string? srcListName);
                        if (dstListNum == null)
                        {
                            if (!string.IsNullOrEmpty(srcListName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Lista de Precios '{srcListName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.PriceListNum = int.Parse(dstListNum);

                    }, "OCRD.ListNum", rule);

                    // % descuento total
                    RuleHelpers.SetIfAllowed(() => bpDst.DiscountPercent = bpSrc.DiscountPercent, "OCRD.Discount", rule);

                    // Límite de crédito y Límite de comprometido
                    RuleHelpers.SetIfAllowed(() => bpDst.CreditLimit = bpSrc.CreditLimit, "OCRD.CreditLine", rule);

                    // Plazo reclamaciones
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstTermCode = MasterDataMapper.MapByDescription(src, dst, table: "ODUT", codeField: "TermCode", descField: @"""TermName""", srcCode: bpSrc.DunningTerm.ToString(), "", out string? srcTermName);
                        if (dstTermCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcTermName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Plazo de Reclamaciones '{srcTermName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.DunningTerm = dstTermCode;

                    }, "OCRD.DunTerm", rule);

                    // Grupos de descuento efectivo
                    RuleHelpers.SetIfAllowed(() => bpDst.DiscountRelations = bpSrc.DiscountRelations, "OCRD.DiscRel", rule);

                    // Precio efectivo
                    RuleHelpers.SetIfAllowed(() => bpDst.EffectivePrice = bpSrc.EffectivePrice, "OCRD.EffecPrice", rule);

                    // Clase de tarjeta crédito
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstCreditCard = MasterDataMapper.MapByDescription(src, dst, table: "OCRC", codeField: "CreditCard", descField: @"""CardName""", srcCode: bpSrc.CreditCardCode.ToString(), "", out string? srcCreditCardName);
                        if (dstCreditCard == null)
                        {
                            if (!string.IsNullOrEmpty(srcCreditCardName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Clase de tarjeta crédito '{srcCreditCardName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.CreditCardCode = int.Parse(dstCreditCard);

                    }, "OCRD.CreditCard", rule);

                    // Número de tarjeta de crédito
                    RuleHelpers.SetIfAllowed(() => bpDst.CreditCardNum = bpSrc.CreditCardNum, "OCRD.CrCardNum", rule);

                    // Fecha de vencimiento
                    RuleHelpers.SetIfAllowed(() => bpDst.ExpirationDate = bpSrc.ExpirationDate, "OCRD.CardValid", rule);

                    // Número ID
                    RuleHelpers.SetIfAllowed(() => bpDst.OwnerIDNumber = bpSrc.OwnerIDNumber, "OCRD.OwnerIdNum", rule);

                    // Retraso promedio de pago
                    RuleHelpers.SetIfAllowed(() => bpDst.AvarageLate = bpSrc.AvarageLate, "OCRD.AvarageLate", rule);

                    // Prioridad
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstPriority = MasterDataMapper.MapByDescription(src, dst, table: "OBPP", codeField: "PrioCode", descField: @"""PrioDesc""", srcCode: bpSrc.Priority.ToString(), "", out string? srcPrioDesc);
                        if (dstPriority == null)
                        {
                            if (!string.IsNullOrEmpty(srcPrioDesc))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Prioridad '{srcPrioDesc}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.Priority = int.Parse(dstPriority);

                    }, "OCRD.Priority", rule);

                    // IBAN estándar
                    RuleHelpers.SetIfAllowed(() => bpDst.IBAN = bpSrc.IBAN, "OCRD.IBAN", rule);

                    // Feriados ??????????????????????????
                    //RuleHelpers.SetIfAllowed(() => bpDst.hl = bpSrc.Holidays, "OCRD.HldCode", rule);

                    // Fechas de pago ??????????????????????????
                    //RuleHelpers.SetIfAllowed(() => bpDst.BPPaymentDates.PaymentDate = bpSrc.BPPaymentDates.PaymentDate, "OCRD.AvarageLate", rule);

                    // Permitir entrega parcial del pedido
                    RuleHelpers.SetIfAllowed(() => bpDst.PartialDelivery = bpSrc.PartialDelivery, "OCRD.PartDelivr", rule);

                    // Permitir entrega parcial por filas
                    RuleHelpers.SetIfAllowed(() => bpDst.BackOrder = bpSrc.BackOrder, "OCRD.BackOrder", rule);

                    // No aplicar grupos de descuento
                    RuleHelpers.SetIfAllowed(() => bpDst.NoDiscounts = bpSrc.NoDiscounts, "OCRD.NoDiscount", rule);

                    // Cheques que se pueden endosar desde este SN
                    RuleHelpers.SetIfAllowed(() => bpDst.EndorsableChecksFromBP = bpSrc.EndorsableChecksFromBP, "OCRD.EdrsFromBP", rule);

                    // Este SN acepta cheques endosados
                    RuleHelpers.SetIfAllowed(() => bpDst.AcceptsEndorsedChecks = bpSrc.AcceptsEndorsedChecks, "OCRD.EdrsToBP", rule);

                    // Tipo de cambio para pagos recibidos
                    RuleHelpers.SetIfAllowed(() => bpDst.ExchangeRateForIncomingPayment = bpSrc.ExchangeRateForIncomingPayment, "OCRD.EnERD4In", rule);

                    // Tipo de cambio para pagos efectuados
                    RuleHelpers.SetIfAllowed(() => bpDst.ExchangeRateForOutgoingPayment = bpSrc.ExchangeRateForOutgoingPayment, "OCRD.EnERD4Out", rule);

                    #endregion


                    #region  SETTERS DE DATOS - SOLAPA EJECUCIÓN DE PAGO

                    // País/Región
                    RuleHelpers.SetIfAllowed(() => bpDst.HouseBankCountry = bpSrc.HouseBankCountry, "OCRD.HousBnkCry", rule);

                    // Banco
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstHouseBank = MasterDataMapper.MapByDescription(src, dst, table: "ODSC", codeField: "BankCode", descField: @"""BankName""", srcCode: bpSrc.HouseBank, "", out string? srcBankName);
                        if (dstHouseBank == null)
                        {
                            if (!string.IsNullOrEmpty(srcBankName))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Banco '{srcBankName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.HouseBank = dstHouseBank;

                    }, "OCRD.HouseBank", rule);

                    // Cuenta bancaria
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstHouseBankAcct = MasterDataMapper.MapByDescription(src, dst, table: "DSC1", codeField: "Account", descField: @"""Account""", srcCode: bpSrc.HouseBankAccount, "", out string? srcHouseBankAccount);

                        if (dstHouseBankAcct == null)
                        {
                            if (!string.IsNullOrEmpty(srcHouseBankAccount))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Cuenta bancaria '{srcHouseBankAccount}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.HouseBankAccount = dstHouseBankAcct;

                    }, "OCRD.HousBnkAct", rule);

                    // IBAN de la cuenta bancaria {GET} ??????????????????????

                    // Info detallada de referencia
                    RuleHelpers.SetIfAllowed(() => bpDst.ReferenceDetails = bpSrc.ReferenceDetails, "OCRD.RefDetails", rule);

                    // Bloqueo de pago
                    RuleHelpers.SetIfAllowed(() => bpDst.PaymentBlock = bpSrc.PaymentBlock, "OCRD.PaymBlock", rule);

                    // Pago unico
                    RuleHelpers.SetIfAllowed(() => bpDst.PaymentBlock = bpSrc.PaymentBlock, "OCRD.SinglePaym", rule);

                    // Autorización de consolidación
                    RuleHelpers.SetIfAllowed(() => bpDst.CollectionAuthorization = bpSrc.CollectionAuthorization, "OCRD.CollecAuth", rule);

                    // Código de imputación de los gastos bancarios
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        string? dstBankChargesAllocationCode = MasterDataMapper.MapByDescription(src, dst, table: "OBCA", codeField: "Code", descField: @"""Name""", srcCode: bpSrc.BankChargesAllocationCode, "", out string? srcBankChargesAllocationCode);

                        if (dstBankChargesAllocationCode == null)
                        {
                            if (!string.IsNullOrEmpty(srcBankChargesAllocationCode))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Código de imputación de los gastos bancarios '{srcBankChargesAllocationCode}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.");
                            }
                            return;
                        }
                        bpDst.BankChargesAllocationCode = dstBankChargesAllocationCode;

                    }, "OCRD.BCACode", rule);

                    // Calcular automáticamente gastos bancarios para pagos recibidos
                    RuleHelpers.SetIfAllowed(() => bpDst.AutomaticPosting = bpSrc.AutomaticPosting, "OCRD.AutoCalBCG", rule);

                    #endregion






                    #region OTROS CAMPOS
                    RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_VATCtg").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_VATCtg").Value, "OCRD.U_B1SYS_VATCtg", rule);
                    RuleHelpers.SetIfAllowed(() => bpDst.VatGroup = bpSrc.VatGroup, "OCRD.VatGroup", rule);
                    
                    
                    
                    
                    
                    
                    
                    
                    
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

