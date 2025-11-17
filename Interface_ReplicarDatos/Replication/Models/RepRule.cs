using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication.Models
{

    public class RepRule
    {
        public string Code { get; set; }          // PHXA>PHXB>OCRD_S
        public string SrcDB { get; set; }         // "PHXA"
        public string DstDB { get; set; }         // "PHXB", "MUNDOBB", "ML"
        public string Table { get; set; }         // "OCRD" / "OITM" / "ITM1"
        public string FilterSQL { get; set; }     // WHERE extra, p.ej CardType='S'
        public string ExcludeCSV { get; set; }    // lista "OCRD.GroupNum,OCRD.CreateDate"
        public string AssignJSON { get; set; }    // JSON { "OCRD.GroupCode": "800" }
        public bool Active { get; set; }

        // Del audio / diseño:
        public string RepBPType { get; set; }     // P=Prov, C=Cli, B=Ambos
        public bool UseBPProperty { get; set; }   // Y/N
        public string BPPropertyCode { get; set; }// nombre flag/UDF, ej. U_Replicate

        private string[] _excludeFields;
        public string[] ExcludeFields
        {
            get
            {
                if (_excludeFields != null) return _excludeFields;
                if (string.IsNullOrWhiteSpace(ExcludeCSV)) return _excludeFields = Array.Empty<string>();

                var parts = ExcludeCSV.Split(',');
                for (int i = 0; i < parts.Length; i++)
                    parts[i] = parts[i].Trim().ToUpperInvariant();

                return _excludeFields = parts;
            }
        }

        private Dictionary<string, string> _assignDict;
        private Dictionary<string, string> AssignDict
        {
            get
            {
                if (_assignDict != null) return _assignDict;
                if (string.IsNullOrWhiteSpace(AssignJSON))
                {
                    _assignDict = new Dictionary<string, string>();
                }
                else
                {
                    try
                    {
                        _assignDict = JsonSerializer.Deserialize<Dictionary<string, string>>(AssignJSON)
                                                              ?? new Dictionary<string, string>();
                    }
                    catch
                    {
                        _assignDict = new Dictionary<string, string>();
                    }
                }
                return _assignDict;
            }
        }

        public T AssignOrDefault<T>(string logicalFieldName, T original)
        {
            // logicalFieldName ej.: "OCRD.GroupCode"
            if (AssignDict.TryGetValue(logicalFieldName, out var forced))
            {
                try
                {
                    return (T)Convert.ChangeType(forced, typeof(T));
                }
                catch
                {
                    return original;
                }
            }
            return original;
        }
    }

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

