using Microsoft.Extensions.Configuration;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Interface_ReplicarDatos
{
    public static class QuartzExtensions
    {
        public static void AddJobAndTrigger<T>(
          this IServiceCollectionQuartzConfigurator quartz,
          IConfiguration config)
          where T : IJob
        {
            
            string nomeJob = typeof(T).Name;

            var configKey = $"Quartz:{nomeJob}";
            var expressionCron = config[configKey];

            if (string.IsNullOrEmpty(expressionCron))
            {
                throw new Exception($"No Quartz.NET Cron schedule found for job in configuration at {configKey}");
            }
         
            var jobKey = new JobKey(nomeJob);
            quartz.AddJob<T>(opts => opts.WithIdentity(jobKey));

            quartz.AddTrigger(opts => opts
                .ForJob(jobKey)
                .WithIdentity(nomeJob + "-trigger")
                .WithCronSchedule(expressionCron));
                
        }
    }
}
