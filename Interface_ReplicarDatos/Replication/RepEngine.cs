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
        void RunOcrdReplication(); // Método para replicación de socios de negocio (OCRD)
        void RunOitmReplication(); // Método para replicación de artículos (OITM)
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
                // PHXA (PMX_TEST) es la base madre donde viven @GNA_REP_CFG y @GNA_REP_FMAP
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
                    // Por diseño, SrcDB debería ser siempre "PHXA" (base madre),
                    // pero lo sacamos de la regla por si en el futuro se amplía.
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
                // PHXA (PMX_TEST) es la base madre donde viven @GNA_REP_CFG y @GNA_REP_FMAP
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
                    // Por diseño, SrcDB debería ser siempre "PHXA" (base madre),
                    // pero lo sacamos de la regla por si en el futuro se amplía.
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
    }
}
