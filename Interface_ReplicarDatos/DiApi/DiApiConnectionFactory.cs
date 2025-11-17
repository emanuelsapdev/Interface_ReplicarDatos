using Interface_ReplicarDatos.Configuration;
using Microsoft.Extensions.Options;
using SAPbobsCOM;
using System;
using System.Runtime.InteropServices;

public interface IDiApiConnectionFactory
{
    Company Connect(string companyKey);   // "PHXA", "PHXB", etc.
    void Disconnect(Company company);
}

public class DiApiConnectionFactory : IDiApiConnectionFactory
{
    private readonly SapCompaniesConfig _companies;

    public DiApiConnectionFactory(IOptions<SapCompaniesConfig> options)
    {
        _companies = options.Value;
    }

    public Company Connect(string companyKey)
    {
        if (!_companies.TryGetValue(companyKey, out var cfg))
            throw new Exception($"No hay configuración DI API para la empresa '{companyKey}'.");

        var cmp = new Company
        {
            Server = cfg.Server,
            CompanyDB = cfg.CompanyDB,
            UserName = cfg.UserName,
            Password = cfg.Password,
            DbUserName = cfg.DbUserName,
            DbPassword = cfg.DbPassword,
            DbServerType = BoDataServerTypes.dst_HANADB,
            language = BoSuppLangs.ln_Spanish_La,
            UseTrusted = false
        };

        int ret = cmp.Connect();
        if (ret != 0)
        {
            cmp.GetLastError(out int code, out string errMsg);
            throw new Exception($"Error conectando a {companyKey} ({cfg.CompanyDB}): {code} - {errMsg}");
        }

        return cmp;
    }

    public void Disconnect(Company company)
    {
        if (company == null) return;

        try
        {
            if (company.Connected)
                company.Disconnect();
        }
        catch
        {
            // Podés loguear el fallo si querés
        }
        finally
        {
            Marshal.ReleaseComObject(company);
        }
    }
}
