using Interface_ReplicarDatos;
using Interface_ReplicarDatos.Configuration;
using Interface_ReplicarDatos.Jobs;
using Interface_ReplicarDatos.Replication;
using NLog;
using NLog.Web;
using Quartz;

var logger = NLog.LogManager.Setup().LoadConfigurationFromAppSettings().GetCurrentClassLogger();
logger.Debug("Initial main");

try
{


IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        // Seteo las configs en las clases de configuración
        services.Configure<SapCompaniesConfig>(hostContext.Configuration.GetSection("SapCompanies"));

        services.AddQuartz(q =>
        {
            q.UseMicrosoftDependencyInjectionJobFactory();

            // Configuro el Job de Sync
            q.AddJobAndTrigger<OcrdReplicationJob>(hostContext.Configuration);
            q.AddJobAndTrigger<OitmReplicationJob>(hostContext.Configuration);
            q.AddJobAndTrigger<OitmPriceListReplicationJob>(hostContext.Configuration);
        });

        // Agrego los servicios
        services.AddSingleton<IRepEngine, RepEngine>();
        services.AddSingleton<IDiApiConnectionFactory, DiApiConnectionFactory>();

        // Agrego el Job de Sync
        services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);
    }).ConfigureLogging(logging =>
    {
        logging.ClearProviders();

    }).UseNLog().UseWindowsService().Build();

await host.RunAsync();
}
catch (Exception ex)
{
    logger.Error(ex, "Fallo al iniciar el servicio");
    throw;
}
finally
{
    LogManager.Shutdown();              
}