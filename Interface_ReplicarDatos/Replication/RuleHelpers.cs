using Interface_ReplicarDatos.Replication.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication
{
    public static class RuleHelpers
    {
        // Solo ejecuta el setter si el campo NO está excluido en la regla
        public static void SetIfAllowed(Action setter, string logicalFieldName, RepRule rule)
        {
            // logicalFieldName: "OCRD.CardName", "OCRD.GroupCode", etc.
            string normalized = logicalFieldName.Trim().ToUpperInvariant();
            foreach (var f in rule.ExcludeFields)
            {
                if (f == normalized)
                    return; // excluido → no se asigna
            }

            setter();
        }
    }
}
