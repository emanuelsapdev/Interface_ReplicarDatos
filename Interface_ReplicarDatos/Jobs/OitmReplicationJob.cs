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
    public class OitmReplicationJob : IJob
    {
        private readonly ILogger<OitmReplicationJob> _logger;
        private readonly IRepEngine _engine;

        public OitmReplicationJob(ILogger<OitmReplicationJob> logger, IRepEngine engine)
        {
            _logger = logger;
            _engine = engine;
        }

        public Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation("Comenzando replica de Items a las {time}", DateTimeOffset.Now);
            _engine.RunOitmReplication();
            return Task.CompletedTask;
        }
    }
}