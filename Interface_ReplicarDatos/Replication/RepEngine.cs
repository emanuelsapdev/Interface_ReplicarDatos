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
    public interface IRepEngine
    {
        void RunOcrdReplication();      // Método para replicación de socios de negocio (OCRD)
        void RunOitmReplication();      // Método para replicación de artículos (OITM)
        void RunOitmPriceList();        // Método para replicación de listas de precios (ITM1)
    }

    public class RepEngine : IRepEngine
    {
        private readonly IDiApiConnectionFactory _factory;

        public RepEngine(IDiApiConnectionFactory factory)
        {
            _factory = factory;
        }

        /// <summary>
        /// Ejecuta todas las reglas activas de OCRD configuradas en @GNA_REP_CFG.
        /// </summary>
        public void RunOcrdReplication()
        {
            Company cfgCmp = null;

            try
            {
                // PHXA (PMX_TEST) es la base madre donde viven @GNA_REP_CFG
                cfgCmp = _factory.Connect("PHXA");

                //0) Infraestructura necesaria
                InfraInstaller.InstallInCompany(cfgCmp);

                //1) Cargar mapeos de campos
                FieldMappingService.LoadAll(cfgCmp);

                //2) Cargar reglas activas para OCRD
                List<RepRule> rules = RepRuleLoader.LoadActiveRules(cfgCmp, "OCRD");

                //3) Ejecutar cada regla
                foreach (var rule in rules)
                {
                    OcrdReplicator.Run(rule, _factory);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                _factory.Disconnect(cfgCmp);
            }
        }

        /// <summary>
        /// Ejecuta todas las reglas activas de OITM configuradas en @GNA_REP_CFG.
        /// </summary>
        public void RunOitmReplication()
        {
            Company cfgCmp = null;

            try
            {
                cfgCmp = _factory.Connect("PHXA");

                //0) Infraestructura necesaria
                InfraInstaller.InstallInCompany(cfgCmp);

                //1) Cargar mapeos de campos
                FieldMappingService.LoadAll(cfgCmp);

                //2) Cargar reglas activas para OITM
                List<RepRule> rules = RepRuleLoader.LoadActiveRules(cfgCmp, "OITM");

                //3) Ejecutar cada regla
                foreach (var rule in rules)
                {
                    OitmReplicator.Run(rule, _factory);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                _factory.Disconnect(cfgCmp);
            }
        }

        /// <summary>
        /// Ejecuta todas las reglas activas de listas de precios de artículos (ITM1) configuradas en @GNA_REP_CFG.
        /// </summary>
        public void RunOitmPriceList()
        {
            Company cfgCmp = null;

            try
            {
                // PHXA (PMX_TEST) es la base madre donde viven @GNA_REP_CFG
                cfgCmp = _factory.Connect("PHXA");

                //0) Infraestructura necesaria
                InfraInstaller.InstallInCompany(cfgCmp);

                //1) Cargar mapeos de campos
                FieldMappingService.LoadAll(cfgCmp);

                //2) Cargar reglas activas para ITM1 / listas de precios
                //    Puedes reutilizar "OITM" o definir un objeto lógico distinto, por ejemplo "ITM1".
                List<RepRule> rules = RepRuleLoader.LoadActiveRules(cfgCmp, "ITM1");

                //3) Ejecutar cada regla
                foreach (var rule in rules)
                {
                    OitmPriceListReplicator.Run(rule, _factory);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                _factory.Disconnect(cfgCmp);
            }
        }
    }
}
