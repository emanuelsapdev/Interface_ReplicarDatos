using Interface_ReplicarDatos.Replication;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos.Jobs
{

    [DisallowConcurrentExecution]
    public class OitmPriceListReplicationJob : IJob
    {
        private readonly ILogger<OitmPriceListReplicationJob> _logger;
        private readonly IRepEngine _engine;

        public OitmPriceListReplicationJob(ILogger<OitmPriceListReplicationJob> logger, IRepEngine engine)
        {
            _logger = logger;
            _engine = engine;
        }

        public Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation("Comenzando replica de Lista de Precios a las {time}", DateTimeOffset.Now);
            _engine.RunOitmPriceList();
            return Task.CompletedTask;
        }

    }
}