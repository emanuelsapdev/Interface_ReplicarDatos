using Interface_ReplicarDatos.Replication.Models;
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
        void RunOcrdReplication();
    }
    public class RepEngine : IRepEngine
    {
        private readonly IDiApiConnectionFactory _factory;

        public RepEngine(IDiApiConnectionFactory factory)
        {
            _factory = factory;
        }

        /// <summary>
        /// Ejecuta todas las reglas activas de OCRD configuradas en @REP_CFG.
        /// </summary>
        public void RunOcrdReplication()
        {
            Company cfgCmp = null;

            try
            {
                // PHXA (PMX_TEST) es la base madre donde viven @REP_CFG y @REP_FMAP
                cfgCmp = _factory.Connect("PHXA");

                // 1) Cargar mapeos de campos
                FieldMappingService.LoadAll(cfgCmp);

                // 2) Cargar reglas activas para OCRD
                List<RepRule> rules = RepRuleLoader.LoadActiveRules(cfgCmp, "OCRD");

                // 3) Ejecutar cada regla
                foreach (var rule in rules)
                {
                    // Por diseño, SrcDB debería ser siempre "PHXA" (base madre),
                    // pero lo sacamos de la regla por si en el futuro se amplía.
                    OcrdReplicator.Run(rule, _factory);
                }
            }
            finally
            {
                _factory.Disconnect(cfgCmp);
            }
        }
    }
}
