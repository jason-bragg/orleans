using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Providers;
using Orleans.Runtime.Scheduler;

namespace Orleans.Providers.Legacy
{
    internal class BootstrapProviderLifecycle : ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly ILogger<BootstrapProviderLifecycle> logger;
        private readonly Lazy<GlobalConfiguration> globalConfig;
        private readonly Lazy<OrleansTaskScheduler> scheduler;
        private readonly Lazy<SiloProviderRuntime> siloProviderRuntime;
        private readonly Lazy<ProviderManagerSystemTarget> providerManagerSystemTarget;
        private readonly Lazy<BootstrapProviderManager> bootstrapProviderManager;

        public BootstrapProviderLifecycle(IServiceProvider services, ILoggerFactory loggerFactory)
        {
            this.logger = loggerFactory.CreateLogger<BootstrapProviderLifecycle>();
            this.globalConfig = new Lazy<GlobalConfiguration>(() => services.GetRequiredService<GlobalConfiguration>());
            this.scheduler = new Lazy<OrleansTaskScheduler>(() => services.GetRequiredService<OrleansTaskScheduler>());
            this.siloProviderRuntime = new Lazy<SiloProviderRuntime>(() => services.GetRequiredService<SiloProviderRuntime>());
            this.providerManagerSystemTarget = new Lazy<ProviderManagerSystemTarget>(() => services.GetRequiredService<ProviderManagerSystemTarget>());
            this.bootstrapProviderManager = new Lazy<BootstrapProviderManager>(() => services.GetRequiredService<BootstrapProviderManager>());
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(SiloLifecycleStage.ApplicationServices, OnApplicationServicesStart, OnApplicationServicesStop);
        }

        private async Task OnApplicationServicesStart(CancellationToken ct)
        {
            await this.scheduler.Value.QueueTask(
                () => this.bootstrapProviderManager.Value.LoadAppBootstrapProviders(siloProviderRuntime.Value, this.globalConfig.Value.ProviderConfigurations),
                this.providerManagerSystemTarget.Value.SchedulingContext)
                    .WithTimeout(InitTimeout());
            logger.Debug("App bootstrap calls done successfully.");
        }

        private async Task OnApplicationServicesStop(CancellationToken ct)
        {
            await this.scheduler.Value.QueueTask(() => this.bootstrapProviderManager.Value.CloseProviders(), providerManagerSystemTarget.Value.SchedulingContext)
                        .WithTimeout(InitTimeout());
        }

        private TimeSpan InitTimeout()
        {
            return Debugger.IsAttached
                ? StandardExtensions.Max(TimeSpan.FromMinutes(10), this.globalConfig.Value.MaxJoinAttemptTime)
                : this.globalConfig.Value.MaxJoinAttemptTime;
        }
    }
}
