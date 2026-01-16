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
                            ""UpdateDate"",
                            ""UpdateTS""
                            FROM ""OCRD"" OCRD
                            WHERE (OCRD.""UpdateDate"" > '{cp.LastDate:yyyy-MM-dd}'
                            OR (OCRD.""UpdateDate"" = '{cp.LastDate:yyyy-MM-dd}' AND OCRD.""UpdateTS"" > {cp.LastTime}))";

                // Si la regla usa propiedad de replicación y el flag es U_Replicate, filtramos por U_Replicate en OCRD
                if (rule.UseRepProperty && !string.IsNullOrWhiteSpace(rule.RepPropertyCode))
                {
                    sql += @$" AND IFNULL(OCRD.""{rule.RepPropertyCode}"", 'Y') = 'Y'";
                }

                if (!string.IsNullOrWhiteSpace(rule.FilterSQL))
                {
                    sql += $" AND ({rule.FilterSQL})";
                }

                rs.DoQuery(sql);

                var bpSrc = (BusinessPartners)src.GetBusinessObject(BoObjectTypes.oBusinessPartners);

                var bpDst = (BusinessPartners)dst.GetBusinessObject(BoObjectTypes.oBusinessPartners);

                while (!rs.EoF)
                {
                    // Leer campos de la base mandatoria
                    string cardCode = rs.Fields.Item(0).Value.ToString();
                    bpSrc.GetByKey(cardCode); // cargar BP origen si es necesario

                    if (!dst.InTransaction)
                        dst.StartTransaction();

                    bool existsBp = bpDst.GetByKey(cardCode); // 

                    if (!existsBp)
                    {
                        bpDst.CardCode = bpSrc.CardCode;
                        bpDst.CardType = bpSrc.CardType;
                    }

                    #region SETTERS DE DATOS - CABECERA

                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Nombre del socio de negocios
                        RuleHelpers.SetIfAllowed(() => bpDst.CardName = bpSrc.CardName, "OCRD.CardName", rule);

                        // Nombre Extranjero del socio de negocios
                        RuleHelpers.SetIfAllowed(() => bpDst.CardForeignName = bpSrc.CardForeignName, "OCRD.CardFName", rule);

                        // Grupo de Cliente / Proveedor
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            if (bpSrc.GroupCode ==0)        
                            return;

                            string? dstGroupCode = MasterDataMapper.MapByDescription(src, dst, table: "OCRG", codeField: "GroupCode", descField: @"""GroupName""", srcCode: bpSrc.GroupCode.ToString(), "", out string? srcGroupName);
                            if (dstGroupCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcGroupName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Grupo de Socio de Negocios '{srcGroupName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.GroupCode");
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

                    }, "OCRD.HEADER", rule);

                    #endregion

                    #region SETTERS DE DATOS - SOLAPA GENERAL
                    RuleHelpers.SetIfAllowed(() =>
                    {
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
                            var srcShipType = bpSrc.ShippingType;
                            if (srcShipType ==0)
                            return;

                            string? dstShipType = MasterDataMapper.MapByDescription(src, dst, table: "OSHP", codeField: "TrnspCode", descField: @"""TrnspName""", srcCode: srcShipType.ToString(), extensionWhereSQL: string.Empty, out string? srcShipTypeName);
                            if (dstShipType == null)
                            {
                                if (!string.IsNullOrEmpty(srcShipTypeName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Tipo de Envío '{srcShipTypeName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.ShipType");
                                }
                                return;
                            }
                            bpDst.ShippingType = int.Parse(dstShipType);
                        }, "OCRD.ShipType", rule);

                        // Indicador de factoring
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            if (string.IsNullOrEmpty(bpSrc.Indicator))
                            return;

                            string? dstIndicator = MasterDataMapper.MapByDescription(src, dst, table: "OIDC", codeField: "Code", descField: @"""Name""", srcCode: bpSrc.Indicator, "", out string? srcIndicator);
                            if (dstIndicator == null)
                            {
                                if (!string.IsNullOrEmpty(srcIndicator))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Indicador '{srcIndicator}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.Indicator");
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
                            if (string.IsNullOrEmpty(bpSrc.ProjectCode))
                            return;

                            string? dstProjectCode = MasterDataMapper.MapByDescription(src, dst, table: "OPRJ", codeField: "PrjCode", descField: @"""PrjName""", srcCode: bpSrc.ProjectCode, "", out string? srcProjectName);
                            if (dstProjectCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcProjectName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Proyecto de Socio de Negocios '{srcProjectName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.ProjectCod");
                                }
                                return;
                            }
                            bpDst.ProjectCode = dstProjectCode;
                        }, "OCRD.ProjectCod", rule);

                        // Industria
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcIndustry = bpSrc.Industry;
                            if (srcIndustry ==0)
                            return;

                            string? dstIndustryCode = MasterDataMapper.MapByDescription(src, dst, table: "OOND", codeField: "IndCode", descField: @"""IndName""", srcCode: srcIndustry.ToString(), "", out string? srcIndustryName);
                            if (dstIndustryCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcIndustryName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Industria de Socio de Negocios '{srcIndustryName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.IndustryC");
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
                            if (string.IsNullOrEmpty(bpSrc.ContactPerson))
                            return;

                            string? dstContactPerson = MasterDataMapper.MapByDescription(src, dst, table: "OCPR", codeField: "Name", descField: @"""Name""", srcCode: bpSrc.ContactPerson, extensionWhereSQL: @$"""CardCode"" = '{bpSrc.CardCode}'", out string? srcContactPerson);
                            if (dstContactPerson == null)
                            {
                                if (!string.IsNullOrEmpty(srcContactPerson))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Persona de Contacto '{srcContactPerson}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.CntctPrsn");
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
                            var srcSlpCode = bpSrc.SalesPersonCode;
                            if (srcSlpCode == -1)
                            return;

                            string? dstSlpCode = MasterDataMapper.MapByDescription(src, dst, table: "OSLP", codeField: "SlpCode", descField: @"""SlpName""", srcCode: srcSlpCode.ToString(), "", out string? srcSlpName);
                            if (dstSlpCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcSlpName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Encargado de Venta/Compra '{srcSlpName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.SlpCode");
                                }
                                return;
                            }
                            bpDst.SalesPersonCode = int.Parse(dstSlpCode);

                        }, "OCRD.SlpCode", rule);

                        // Responsable
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcAgent = bpSrc.AgentCode;
                            if (string.IsNullOrEmpty(srcAgent))
                            return;

                            string? dstAgentCode = MasterDataMapper.MapByDescription(src, dst, table: "OAGP", codeField: "AgentCode", descField: @"""AgentName""", srcCode: srcAgent.ToString(), "", out string? srcAgentName);

                            if (dstAgentCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcAgentName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Agente '{srcAgentName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.AgentCode");
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
                            var srcTech = bpSrc.DefaultTechnician;
                            if (srcTech ==0)
                            return;

                            string? dstTechnicalCode = MasterDataMapper.MapByDescription(src, dst, table: "OHEM", codeField: "empID", descField: @"""firstName"" || ""lastName""", srcCode: srcTech.ToString(), "", out string? srcTechnicalName);

                            if (dstTechnicalCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcTechnicalName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Técnico '{srcTechnicalName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.DfTcnician");
                                }
                                return;
                            }

                            bpDst.DefaultTechnician = int.Parse(dstTechnicalCode);

                        }, "OCRD.DfTcnician", rule);

                        // Territorio
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcTerritory = bpSrc.Territory;
                            if (srcTerritory ==0)
                            return;

                            string? dstTerritoryID = MasterDataMapper.MapByDescription(src, dst, table: "OTER", codeField: "territryID", descField: @"""descript""", srcCode: srcTerritory.ToString(), "", out string? srcTerritoryName);
                            if (dstTerritoryID == null)
                            {
                                if (!string.IsNullOrEmpty(srcTerritoryName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Territorio de Socio de Negocios '{srcTerritoryName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.Territory");
                                }
                                return;
                            }
                            bpDst.Territory = int.Parse(dstTerritoryID);

                        }, "OCRD.Territory", rule);

                        // Idioma
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            var srcLang = bpSrc.LanguageCode;
                            if (srcLang ==0)
                            return;

                            string? dstLangCode = MasterDataMapper.MapByDescription(src, dst, table: "OLNG", codeField: "Code", descField: @"""Name""", srcCode: srcLang.ToString(), "", out string? srcLangName);
                            if (dstLangCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcLangName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Lenguaje de Socio de Negocios '{srcLangName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.LngCode");
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
                    }, "OCRD.FLAP_GENERAL", rule);
                    #endregion

                    #region SETTERS DE DATOS - SOLAPA PERSONAS DE CONTACTO
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        for (int i =0; i < bpSrc.ContactEmployees.Count; i++)
                        {
                            bpSrc.ContactEmployees.SetCurrentLine(i);
                            var srcContact = bpSrc.ContactEmployees;
                            // Buscar si el contacto ya existe en el destino
                            var dstContact = bpDst.ContactEmployees;
                            bool contactExists = false;
                            for (int j =0; j < dstContact.Count; j++)
                            {
                                dstContact.SetCurrentLine(j);
                                var existingContact = dstContact;
                                if (existingContact.Name == srcContact.Name)
                                {
                                    // Actualizar contacto existente
                                    contactExists = true;
                                    RuleHelpers.SetIfAllowed(() => existingContact.FirstName = srcContact.FirstName, "OCRD.OCPR.FirstName", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.MiddleName = srcContact.MiddleName, "OCRD.OCPR.MiddleName", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.LastName = srcContact.LastName, "OCRD.OCPR.LastName", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Title = srcContact.Title, "OCRD.OCPR.Title", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Position = srcContact.Position, "OCRD.OCPR.Position", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Address = srcContact.Address, "OCRD.OCPR.Address", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Phone1 = srcContact.Phone1, "OCRD.OCPR.Tel1", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Phone2 = srcContact.Phone2, "OCRD.OCPR.Tel2", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.MobilePhone = srcContact.MobilePhone, "OCRD.OCPR.Cellolar", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Fax = srcContact.Fax, "OCRD.OCPR.Fax", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.E_Mail = srcContact.E_Mail, "OCRD.OCPR.E_MailL", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.EmailGroupCode = srcContact.EmailGroupCode, "OCRD.OCPR.E_MailL", rule);
                                    RuleHelpers.SetIfAllowed(() =>
                                    {
                                        if (string.IsNullOrEmpty(srcContact.EmailGroupCode))
                                        return;

                                        string? dstEmailGroupCode = MasterDataMapper.MapByDescription(src, dst, table: "OEGP", codeField: "EmlGrpCode", descField: @"""EmlGrpName""", srcCode: srcContact.EmailGroupCode, "", out string? srcEmlGrpName);
                                        if (dstEmailGroupCode == null)
                                        {
                                            if (!string.IsNullOrEmpty(srcEmlGrpName))
                                            {
                                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Grupo de Correo electronico '{srcEmlGrpName}' (CardCode: {bpSrc.CardCode} - Contact: {srcContact.Name}). Se omite la asignación.", "OCRD.OCPR.EmlGrpCode");
                                            }
                                            return;
                                        }
                                        existingContact.EmailGroupCode = dstEmailGroupCode;

                                    }, "OCRD.OCPR.EmlGrpCode", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Pager = srcContact.Pager, "OCRD.OCPR.Pager", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Remarks1 = srcContact.Remarks1, "OCRD.OCPR.Notes1", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Remarks2 = srcContact.Remarks2, "OCRD.OCPR.Notes2", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Password = srcContact.Password, "OCRD.OCPR.Password", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.PlaceOfBirth = srcContact.PlaceOfBirth, "OCRD.OCPR.BirthPlace", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.DateOfBirth = srcContact.DateOfBirth, "OCRD.OCPR.BirthDate", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Profession = srcContact.Profession, "OCRD.OCPR.Profession", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.CityOfBirth = srcContact.CityOfBirth, "OCRD.OCPR.BirthCity", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.BlockSendingMarketingContent = srcContact.BlockSendingMarketingContent, "OCRD.OCPR.BlockComm", rule);
                                    RuleHelpers.SetIfAllowed(() => existingContact.Active = srcContact.Active, "OCRD.OCPR.Active", rule);

                                    break;
                                }
                            }
                            if (!contactExists)
                            {
                                // Agregar nuevo contacto
                                dstContact.Name = srcContact.Name;
                                RuleHelpers.SetIfAllowed(() => dstContact.FirstName = srcContact.FirstName, "OCRD.OCPR.FirstName", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.MiddleName = srcContact.MiddleName, "OCRD.OCPR.MiddleName", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.LastName = srcContact.LastName, "OCRD.OCPR.LastName", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Title = srcContact.Title, "OCRD.OCPR.Title", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Position = srcContact.Position, "OCRD.OCPR.Position", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Address = srcContact.Address, "OCRD.OCPR.Address", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Phone1 = srcContact.Phone1, "OCRD.OCPR.Tel1", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Phone2 = srcContact.Phone2, "OCRD.OCPR.Tel2", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.MobilePhone = srcContact.MobilePhone, "OCRD.OCPR.Cellolar", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Fax = srcContact.Fax, "OCRD.OCPR.Fax", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.E_Mail = srcContact.E_Mail, "OCRD.OCPR.E_MailL", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.EmailGroupCode = srcContact.EmailGroupCode, "OCRD.OCPR.E_MailL", rule);
                                RuleHelpers.SetIfAllowed(() =>
                                {
                                    if (string.IsNullOrEmpty(srcContact.EmailGroupCode))
                                    return;

                                    string? dstEmailGroupCode = MasterDataMapper.MapByDescription(src, dst, table: "OEGP", codeField: "EmlGrpCode", descField: @"""EmlGrpName""", srcCode: srcContact.EmailGroupCode, "", out string? srcEmlGrpName);
                                    if (dstEmailGroupCode == null)
                                    {
                                        if (!string.IsNullOrEmpty(srcEmlGrpName))
                                        {
                                            LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Grupo de Correo electronico '{srcEmlGrpName}' (CardCode: {bpSrc.CardCode} - Contact: {srcContact.Name}). Se omite la asignación.", "OCRD.OCPR.EmlGrpCode");
                                        }
                                        return;
                                    }
                                    dstContact.EmailGroupCode = dstEmailGroupCode;

                                }, "OCRD.OCPR.EmlGrpCode", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Pager = srcContact.Pager, "OCRD.OCPR.Pager", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Remarks1 = srcContact.Remarks1, "OCRD.OCPR.Notes1", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Remarks2 = srcContact.Remarks2, "OCRD.OCPR.Notes2", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Password = srcContact.Password, "OCRD.OCPR.Password", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.PlaceOfBirth = srcContact.PlaceOfBirth, "OCRD.OCPR.BirthPlace", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.DateOfBirth = srcContact.DateOfBirth, "OCRD.OCPR.BirthDate", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Profession = srcContact.Profession, "OCRD.OCPR.Profession", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.CityOfBirth = srcContact.CityOfBirth, "OCRD.OCPR.BirthCity", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.BlockSendingMarketingContent = srcContact.BlockSendingMarketingContent, "OCRD.OCPR.BlockComm", rule);
                                RuleHelpers.SetIfAllowed(() => dstContact.Active = srcContact.Active, "OCRD.OCPR.Active", rule);

                                dstContact.Add();
                            }
                        }
                    }, "OCRD.FLAP_CONTACTS", rule);
                    #endregion

                    #region SETTERS DE DATOS - SOLAPA DIRECCIONES 
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        for (int i =0; i < bpSrc.Addresses.Count; i++)
                        {
                            bpSrc.Addresses.SetCurrentLine(i);
                            var srcAddress = bpSrc.Addresses;
                            // Buscar si la dirección ya existe en el destino
                            var dstAddress = bpDst.Addresses;
                            bool addressExists = false;
                            for (int j =0; j < dstAddress.Count; j++)
                            {
                                dstAddress.SetCurrentLine(j);
                                var existingAddress = dstAddress;
                                if (existingAddress.AddressName == srcAddress.AddressName)
                                {
                                    // Actualizar dirección existente
                                    addressExists = true;

                                    RuleHelpers.SetIfAllowed(() => existingAddress.AddressName2 = srcAddress.AddressName2, "OCRD.CRD1.Address2", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.AddressName3 = srcAddress.AddressName3, "OCRD.CRD1.Address3", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.Street = srcAddress.Street, "OCRD.CRD1.Street", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.Block = srcAddress.Block, "OCRD.CRD1.Block", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.City = srcAddress.City, "OCRD.CRD1.City", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.ZipCode = srcAddress.ZipCode, "OCRD.CRD1.ZipCode", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.County = srcAddress.County, "OCRD.CRD1.County", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.State = srcAddress.State, "OCRD.CRD1.State", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.Country = srcAddress.Country, "OCRD.CRD1.Country", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.Country = srcAddress.Country, "OCRD.CRD1.TaxCode", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.StreetNo = srcAddress.StreetNo, "OCRD.CRD1.StreetNo", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.BuildingFloorRoom = srcAddress.BuildingFloorRoom, "OCRD.CRD1.Building", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.TaxOffice = srcAddress.TaxOffice, "OCRD.CRD1.TaxOffice", rule);
                                    RuleHelpers.SetIfAllowed(() => existingAddress.GlobalLocationNumber = srcAddress.GlobalLocationNumber, "OCRD.CRD1.GlblLocNum", rule);

                                    break;
                                }
                            }
                            if (!addressExists)
                            {
                                // Agregar nueva dirección
                                dstAddress.AddressName = srcAddress.AddressName;
                                RuleHelpers.SetIfAllowed(() => dstAddress.AddressName2 = srcAddress.AddressName2, "OCRD.CRD1.Address2", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.AddressName3 = srcAddress.AddressName3, "OCRD.CRD1.Address3", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.Street = srcAddress.Street, "OCRD.CRD1.Street", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.Block = srcAddress.Block, "OCRD.CRD1.Block", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.City = srcAddress.City, "OCRD.CRD1.City", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.ZipCode = srcAddress.ZipCode, "OCRD.CRD1.ZipCode", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.County = srcAddress.County, "OCRD.CRD1.County", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.State = srcAddress.State, "OCRD.CRD1.State", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.Country = srcAddress.Country, "OCRD.CRD1.Country", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.Country = srcAddress.Country, "OCRD.CRD1.TaxCode", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.StreetNo = srcAddress.StreetNo, "OCRD.CRD1.StreetNo", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.BuildingFloorRoom = srcAddress.BuildingFloorRoom, "OCRD.CRD1.Building", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.TaxOffice = srcAddress.TaxOffice, "OCRD.CRD1.TaxOffice", rule);
                                RuleHelpers.SetIfAllowed(() => dstAddress.GlobalLocationNumber = srcAddress.GlobalLocationNumber, "OCRD.CRD1.GlblLocNum", rule);
                            }
                        }
                    }, "OCRD.FLAP_ADDRESSES", rule);
                    #endregion

                    #region  SETTERS DE DATOS - SOLAPA CONDICIONES DE PAGO
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Condición de Pago
                        RuleHelpers.SetIfAllowed(() =>
                    {
                        var srcPayTerms = bpSrc.PayTermsGrpCode;
                        if (srcPayTerms ==0)
                            return;

                        string? dstGroupNum = MasterDataMapper.MapByDescription(src, dst, table: "OCTG", codeField: "GroupNum", descField: @"""PymntGroup""", srcCode: srcPayTerms.ToString(), "", out string? srcPymntGroup);
                        if (dstGroupNum == null)
                        {
                            if (!string.IsNullOrEmpty(srcPymntGroup))
                            {
                                LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Condición de Pago '{srcPymntGroup}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.GroupNum");
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
                            var srcPriceList = bpSrc.PriceListNum;
                            if (srcPriceList == -1)
                            return;

                            string? dstListNum = MasterDataMapper.MapByDescription(src, dst, table: "OPLN", codeField: "ListNum", descField: @"""ListName""", srcCode: srcPriceList.ToString(), "", out string? srcListName);
                            if (dstListNum == null)
                            {
                                if (!string.IsNullOrEmpty(srcListName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Lista de Precios '{srcListName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.ListNum");
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
                            var srcDunning = bpSrc.DunningTerm;
                            if (string.IsNullOrEmpty(srcDunning))
                            return;

                            string? dstTermCode = MasterDataMapper.MapByDescription(src, dst, table: "ODUT", codeField: "TermCode", descField: @"""TermName""", srcCode: srcDunning, "", out string? srcTermName);
                            if (dstTermCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcTermName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Plazo de Reclamaciones '{srcTermName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.DunTerm");
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
                            var srcCreditCard = bpSrc.CreditCardCode;
                            if (srcCreditCard ==0)
                            return;

                            string? dstCreditCard = MasterDataMapper.MapByDescription(src, dst, table: "OCRC", codeField: "CreditCard", descField: @"""CardName""", srcCode: srcCreditCard.ToString(), "", out string? srcCreditCardName);
                            if (dstCreditCard == null)
                            {
                                if (!string.IsNullOrEmpty(srcCreditCardName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Clase de tarjeta crédito '{srcCreditCardName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.CreditCard");
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
                            var srcPriority = bpSrc.Priority;
                            if (srcPriority ==0)
                            return;

                            string? dstPriority = MasterDataMapper.MapByDescription(src, dst, table: "OBPP", codeField: "PrioCode", descField: @"""PrioDesc""", srcCode: srcPriority.ToString(), "", out string? srcPrioDesc);
                            if (dstPriority == null)
                            {
                                if (!string.IsNullOrEmpty(srcPrioDesc))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Prioridad '{srcPrioDesc}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.Priority");
                                }
                                return;
                            }
                            bpDst.Priority = int.Parse(dstPriority);

                        }, "OCRD.Priority", rule);

                        // IBAN estándar
                        RuleHelpers.SetIfAllowed(() => bpDst.IBAN = bpSrc.IBAN, "OCRD.IBAN", rule);

                        // Fechas de pago
                        RuleHelpers.SetIfAllowed(() => bpDst.BPPaymentDates.PaymentDate = bpSrc.BPPaymentDates.PaymentDate, "OCRD.BPPaymentDates", rule);

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
                    }, "OCRD.FLAP_PAYMENT_TERMS", rule);
                    #endregion

                    #region  SETTERS DE DATOS - SOLAPA EJECUCIÓN DE PAGO
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // País/Región
                        RuleHelpers.SetIfAllowed(() => bpDst.HouseBankCountry = bpSrc.HouseBankCountry, "OCRD.HousBnkCry", rule);

                        // Banco
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            if (string.IsNullOrEmpty(bpSrc.HouseBank))
                            return;

                            string? dstHouseBank = MasterDataMapper.MapByDescription(src, dst, table: "ODSC", codeField: "BankCode", descField: @"""BankName""", srcCode: bpSrc.HouseBank, "", out string? srcBankName);
                            if (dstHouseBank == null)
                            {
                                if (!string.IsNullOrEmpty(srcBankName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Banco '{srcBankName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.HouseBank");
                                }
                                return;
                            }
                            bpDst.HouseBank = dstHouseBank;

                        }, "OCRD.HouseBank", rule);

                        // Cuenta bancaria
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            if (string.IsNullOrEmpty(bpSrc.HouseBankAccount))
                            return;

                            string? dstHouseBankAcct = MasterDataMapper.MapByDescription(src, dst, table: "DSC1", codeField: "Account", descField: @"""Account""", srcCode: bpSrc.HouseBankAccount, "", out string? srcHouseBankAccount);

                            if (dstHouseBankAcct == null)
                            {
                                if (!string.IsNullOrEmpty(srcHouseBankAccount))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Cuenta bancaria '{srcHouseBankAccount}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.HousBnkAct");
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
                            if (string.IsNullOrEmpty(bpSrc.BankChargesAllocationCode))
                            return;

                            string? dstBankChargesAllocationCode = MasterDataMapper.MapByDescription(src, dst, table: "OBCA", codeField: "Code", descField: @"""Name""", srcCode: bpSrc.BankChargesAllocationCode, "", out string? srcBankChargesAllocationCode);

                            if (dstBankChargesAllocationCode == null)
                            {
                                if (!string.IsNullOrEmpty(srcBankChargesAllocationCode))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Código de imputación de los gastos bancarios '{srcBankChargesAllocationCode}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.BCACode");
                                }
                                return;
                            }
                            bpDst.BankChargesAllocationCode = dstBankChargesAllocationCode;

                        }, "OCRD.BCACode", rule);

                        // Calcular automáticamente gastos bancarios para pagos recibidos
                        RuleHelpers.SetIfAllowed(() => bpDst.AutomaticPosting = bpSrc.AutomaticPosting, "OCRD.AutoCalBCG", rule);
                    }, "OCRD.FLAP_PAYMENT_EXEC", rule);
                    #endregion

                    #region  SETTERS DE DATOS - SOLAPA FINANZAS

                    RuleHelpers.SetIfAllowed(() =>
                    {
                        #region GENERAL
                        // Socio comercial de consolidación
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            if (string.IsNullOrEmpty(bpSrc.FatherCard))
                            return;

                            string? dstFatherCard = MasterDataMapper.MapByDescription(src, dst, table: "OCRD", codeField: "CardCode", descField: @"""CardName""", srcCode: bpSrc.FatherCard, "", out string? srcFatherName);

                            if (dstFatherCard == null)
                            {
                                if (!string.IsNullOrEmpty(srcFatherName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Socio comercial de consolidación '{srcFatherName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.BCACode");
                                }
                                return;
                            }
                            bpDst.FatherCard = dstFatherCard;

                        }, "OCRD.BCACode", rule);

                        // Consolidación de pagos / Consolidaci&ón de entregas
                        RuleHelpers.SetIfAllowed(() => bpDst.FatherType = bpSrc.FatherType, "OCRD.FatherType", rule);

                        // Bloquear reclamaciones
                        RuleHelpers.SetIfAllowed(() => bpDst.BlockDunning = bpSrc.BlockDunning, "OCRD.BlockDunn", rule);

                        // Proveedor conectado
                        RuleHelpers.SetIfAllowed(() =>
                        {
                            if (string.IsNullOrEmpty(bpSrc.LinkedBusinessPartner))
                            return;

                            string? dstLinkBp = MasterDataMapper.MapByDescription(src, dst, table: "OCRD", codeField: "CardCode", descField: @"""CardName""", srcCode: bpSrc.LinkedBusinessPartner, "", out string? srcLinkBpName);

                            if (dstLinkBp == null)
                            {
                                if (!string.IsNullOrEmpty(srcLinkBpName))
                                {
                                    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: bpSrc.CardCode, "WARNING", $"No se encontró mapeo para Socio comercial de consolidación '{srcLinkBpName}' (CardCode: {bpSrc.CardCode}). Se omite la asignación.", "OCRD.ConnBP");
                                }
                                return;
                            }
                            bpDst.LinkedBusinessPartner = dstLinkBp;

                        }, "OCRD.ConnBP", rule);

                        // Grupo de planificación
                        RuleHelpers.SetIfAllowed(() => bpDst.PlanningGroup = bpSrc.PlanningGroup, "OCRD.PlngGroup", rule);

                        // Utilizar cuenta de mercancías enviadas
                        RuleHelpers.SetIfAllowed(() => bpDst.UseShippedGoodsAccount = bpSrc.UseShippedGoodsAccount, "OCRD.UseShpdGd", rule);

                        // Empresa asociada
                        RuleHelpers.SetIfAllowed(() => bpDst.Affiliate = bpSrc.Affiliate, "OCRD.Affiliate", rule);

                        #endregion

                        #region IMPUESTO

                        // Obligat. / Extranjero
                        RuleHelpers.SetIfAllowed(() => bpDst.VatLiable = bpSrc.VatLiable, "OCRD.VatStatus", rule);

                        // Sujeto a retención
                        RuleHelpers.SetIfAllowed(() => bpDst.SubjectToWithholdingTax = bpSrc.SubjectToWithholdingTax, "OCRD.WTLiable", rule);

                        // ID de ingresos brutos
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_GrsIncId").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_GrsIncId").Value, "OCRD.U_B1SYS_GrsIncId", rule);

                        // Categoría de ingresos brutos
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_GrsIncCtg").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_GrsIncCtg").Value, "OCRD.U_B1SYS_GrsIncCtg", rule);

                        // Categoría de IVA
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_VATCtg").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_VATCtg").Value, "OCRD.U_B1SYS_VATCtg", rule);

                        // Categoría de impuesto a las ganancias
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_IncTaxCtg").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_IncTaxCtg").Value, "OCRD.U_B1SYS_IncTaxCtg", rule);

                        // Estado de Reproweb
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_ReproWebSta").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_ReproWebSta").Value, "OCRD.U_B1SYS_ReproWebSta", rule);

                        // Participación accionaria en otras empresas
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_ShareFromO").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_ShareFromO").Value, "OCRD.U_B1SYS_ShareFromO", rule);

                        // Empresario
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_Employer").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_Employer").Value, "OCRD.U_B1SYS_Employer", rule);

                        // Actividad de monotributo
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_MonoAct").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_MonoAct").Value, "OCRD.U_B1SYS_MonoAct", rule);

                        // Categoría de monotributo
                        RuleHelpers.SetIfAllowed(() => bpDst.UserFields.Fields.Item("U_B1SYS_MonoCtg").Value = bpSrc.UserFields.Fields.Item("U_B1SYS_MonoCtg").Value, "OCRD.U_B1SYS_MonoCtg", rule);

                        // Número EORI
                        RuleHelpers.SetIfAllowed(() => bpDst.EORINumber = bpSrc.EORINumber, "OCRD.EORINumber", rule);


                        #endregion
                    }, "OCRD.FLAP_FINANCE", rule);
                    #endregion

                    #region SETTERS DE DATOS - SOLAPA PROPIEDADES
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Nombre de la propiedad
                        for (int i =1; i <=64; i++)
                        {
                            string fieldName = $"QryGroup{i}";
                            RuleHelpers.SetIfAllowed(() => bpDst.Properties[i] = bpSrc.Properties[i], $"OCRD.{fieldName}", rule);
                        }
                    }, "OCRD.FLAP7_PROPERTIES", rule);
                    #endregion

                    #region SETTERS DE DATOS - SOLAPA COMENTARIOS
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Comentario
                        RuleHelpers.SetIfAllowed(() => bpDst.FreeText = bpSrc.FreeText, "OCRD.Free_Text", rule);
                    }, "OCRD.FLAP_COMMENTS", rule);
                    #endregion

                    #region SETTERS DE DATOS - SOLAPA DOCUMENTOS ELECTRÓNICOS
                    RuleHelpers.SetIfAllowed(() =>
                    {
                        // Relevante para FCE
                        RuleHelpers.SetIfAllowed(() => bpDst.FCERelevant = bpSrc.FCERelevant, "OCRD.FCERelevnt", rule);

                        // Validar documentos de entrega base para mes de contabilización e integridad
                        RuleHelpers.SetIfAllowed(() => bpDst.FCEValidateBaseDelivery = bpSrc.FCEValidateBaseDelivery, "OCRD.FCEVldte", rule);

                        // Utilizar FCEs como medios de pago
                        RuleHelpers.SetIfAllowed(() => bpDst.FCEAsPaymentMeans = bpSrc.FCEAsPaymentMeans, "OCRD.FCEPmnMean", rule);
                    }, "OCRD.FLAP_E_DOCUMENTS", rule);
                    #endregion


                    // Add / Update
                    int ret = existsBp ? bpDst.Update() : bpDst.Add();

                    LogService.HandleDiApiResult(src, dst, ret, rule.Code, "OCRD", cardCode);

                    // 5) Actualizar checkpoint con la fila actual

                    if (ret == 0)
                    {
                        CheckpointService.UpdateFromRow(ref cp, rs, "UpdateDate", "UpdateTS");
                        
                        if (dst.InTransaction)
                            dst.EndTransaction(BoWfTransOpt.wf_Commit);
                    } else
                    {
                        if (dst.InTransaction)
                            dst.EndTransaction(BoWfTransOpt.wf_RollBack);
                    }


                        rs.MoveNext();
                }


                // Guardar checkpoint final
                CheckpointService.PersistCheckpoint(src, rule.Code, cp);

                System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(bpSrc);
                System.Runtime.InteropServices.Marshal.ReleaseComObject(bpDst);
            }
            //catch (Exception ex)
            //{
            //    LogService.WriteLog(src, ruleCode: rule.Code, table: rule.Table, key: null, "WARNING", $"Error: {ex.Message}");
            //}
            finally
            {
                factory.Disconnect(dst);
                factory.Disconnect(src);
            }
        }
    }
}

