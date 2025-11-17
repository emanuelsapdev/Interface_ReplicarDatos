using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Configuration
{
    public class SapCompanyConfig
    {
        public string Server { get; set; }
        public string DbUserName { get; set; }
        public string DbPassword { get; set; }
        public string CompanyDB { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
    }
    public class SapCompaniesConfig : Dictionary<string, SapCompanyConfig>
    {
        
    }
}
