using SAPbobsCOM;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Replication
{

    public static class MasterDataMapper
    {
        /// <summary>
        /// Mapea códigos entre dos bases SAP por coincidencia de descripción.
        /// Sirve para OCTG, OCRG, OVTG, OPLN, y cualquier maestro.
        /// </summary>
        /// <param name="src">Company origen</param>
        /// <param name="dst">Company destino</param>
        /// <param name="table">Tabla SAP (ej: "OCTG")</param>
        /// <param name="codeField">Campo código (ej: "GroupNum")</param>
        /// <param name="descField">Campo descripción (ej: "PymntGroup")</param>
        /// <param name="srcCode">Código origen que quiero mapear</param>
        /// <returns>código destino o -1 si no existe</returns>
        public static string? MapByDescription(
            Company src, Company dst,
            string table,
            string codeField,
            string descField,
            string srcCode,
            string extensionWhereSQL,
            out string? srcDesc
            )
        {
            srcDesc = null;
            if (string.IsNullOrEmpty(srcCode))
            {
                return null;
            }
            if(!string.IsNullOrEmpty(extensionWhereSQL))
                extensionWhereSQL = "AND " + extensionWhereSQL;

            Recordset rsSrc = null;
            Recordset rsDst = null;

            try
            {

                rsSrc = (Recordset)src.GetBusinessObject(BoObjectTypes.BoRecordset);
                rsDst = (Recordset)dst.GetBusinessObject(BoObjectTypes.BoRecordset);

                // 1) Obtener descripción en ORIGEN
                string q1 = $@"
                                SELECT {descField}
                                FROM ""{table}""
                                WHERE ""{codeField}"" = '{srcCode}' {extensionWhereSQL}";
                rsSrc.DoQuery(q1);

                if (rsSrc.EoF)
                    return null;

                string desc = rsSrc.Fields.Item(descField).Value.ToString();
                desc = desc.Replace("'", "''");
                srcDesc = desc;
                // 2) Buscar ese texto en la base DESTINO
                string q2 = $@"
                                SELECT ""{codeField}""
                                FROM ""{table}""
                                WHERE {descField} = '{desc}' {extensionWhereSQL}";
                rsDst.DoQuery(q2);

                if (rsDst.EoF) return null;

                string val = Convert.ToString(rsDst.Fields.Item(codeField).Value);
                return val;
            }
            finally
            {
                if (rsSrc != null) System.Runtime.InteropServices.Marshal.ReleaseComObject(rsSrc);
                if (rsDst != null) System.Runtime.InteropServices.Marshal.ReleaseComObject(rsDst);
            }
        }
    }

}
