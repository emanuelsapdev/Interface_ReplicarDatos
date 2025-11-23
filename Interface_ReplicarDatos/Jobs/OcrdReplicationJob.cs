using Interface_ReplicarDatos.Replication;
using Microsoft.Extensions.Logging;
using Quartz;

namespace Interface_ReplicarDatos.Jobs
{
    [DisallowConcurrentExecution]
    public class OcrdReplicationJob : IJob
    {
        private readonly ILogger<OcrdReplicationJob> _logger;
        private readonly IRepEngine _engine;

        public OcrdReplicationJob(ILogger<OcrdReplicationJob> logger, IRepEngine engine)
        {
            _logger = logger;
            _engine = engine;
        }

        public Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation("Comenzando replica de Business Partners a las {time}", DateTimeOffset.Now);
            
            _engine.RunOcrdReplication();
            return Task.CompletedTask;
        }
    }
}
