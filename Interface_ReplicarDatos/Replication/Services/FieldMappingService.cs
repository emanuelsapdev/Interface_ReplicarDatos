using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication.Services
{
    public class FieldMap
    {
        public string FromDB { get; set; }
        public string ToDB { get; set; }
        public string Table { get; set; }   // OCRD / OITM / ITM1
        public string Field { get; set; }   // VatGroup, DfltWH, etc.
        public string SourceVal { get; set; }
        public string TargetVal { get; set; }
        public bool Fixed { get; set; }     // si es true, ignora SourceVal
    }

    public static class FieldMappingService
    {
        private static bool _loaded;
        private static readonly List<FieldMap> _maps = new List<FieldMap>();

        // Cargar TODOS los mapeos una vez (desde PHXA)
        public static void LoadAll(Company cfgCompany)
        {
            if (_loaded) return;

            var rs = (Recordset)cfgCompany.GetBusinessObject(BoObjectTypes.BoRecordset);
            rs.DoQuery(@"
                        SELECT ""U_FromDB"",""U_ToDB"",""U_Table"",""U_Field"",
                               ""U_SourceVal"",""U_TargetVal"",""U_Fixed""
                        FROM ""@GNA_REP_FMAP""");

            while (!rs.EoF)
            {
                _maps.Add(new FieldMap
                {
                    FromDB = rs.Fields.Item("U_FromDB").Value.ToString(),
                    ToDB = rs.Fields.Item("U_ToDB").Value.ToString(),
                    Table = rs.Fields.Item("U_Table").Value.ToString(),
                    Field = rs.Fields.Item("U_Field").Value.ToString(),
                    SourceVal = rs.Fields.Item("U_SourceVal").Value.ToString(),
                    TargetVal = rs.Fields.Item("U_TargetVal").Value.ToString(),
                    Fixed = rs.Fields.Item("U_Fixed").Value.ToString() == "Y"
                });

                rs.MoveNext();
            }

            System.Runtime.InteropServices.Marshal.ReleaseComObject(rs);
            _loaded = true;
        }

        /// <summary>
        /// Devuelve el valor a usar en destino para un campo.
        /// - Si hay mapeo 'Fixed', lo usa siempre.
        /// - Si hay mapeo con SourceVal igual al valor origen, lo transforma.
        /// - Si no hay mapeo, devuelve el valor origen.
        /// </summary>
        public static string Apply(string fromDB, string toDB, string table, string field, string sourceVal)
        {
            string keyFrom = fromDB ?? "";
            string keyTo = toDB ?? "";
            string t = table ?? "";
            string f = field ?? "";
            string src = sourceVal ?? "";

            // 1) Fixed overrides
            foreach (var m in _maps)
            {
                if (m.FromDB == keyFrom && m.ToDB == keyTo &&
                    m.Table == t && m.Field == f && m.Fixed)
                {
                    return m.TargetVal;
                }
            }

            // 2) Mapeo por SourceVal
            foreach (var m in _maps)
            {
                if (m.FromDB == keyFrom && m.ToDB == keyTo &&
                    m.Table == t && m.Field == f &&
                    !m.Fixed && m.SourceVal == src)
                {
                    return m.TargetVal;
                }
            }

            // 3) Sin mapeo → se deja igual
            return sourceVal;
        }
    }
}
