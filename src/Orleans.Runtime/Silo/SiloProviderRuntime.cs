using System;
using System.Threading.Tasks;

using Orleans.Concurrency;
using Orleans.Runtime.ConsistentRing;
using Orleans.Runtime.Scheduler;
using Orleans.Streams;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Runtime.Providers
{
    internal class SiloProviderRuntime : ISiloSideStreamProviderRuntime
    {
        private readonly ISiloStatusOracle siloStatusOracle;
        private readonly OrleansTaskScheduler scheduler;
        private readonly ActivationDirectory activationDirectory;
        private readonly IConsistentRingProvider consistentRingProvider;
        private readonly ISiloRuntimeClient runtimeClient;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger logger;
        public IGrainFactory GrainFactory => this.runtimeClient.InternalGrainFactory;
        public IServiceProvider ServiceProvider => this.runtimeClient.ServiceProvider;

        public string ServiceId { get; }
        public string SiloIdentity { get; }

        public SiloProviderRuntime(
            ILocalSiloDetails siloDetails,
            IOptions<ClusterOptions> clusterOptions,
            IConsistentRingProvider consistentRingProvider,
            ISiloRuntimeClient runtimeClient,
            ImplicitStreamSubscriberTable implicitStreamSubscriberTable,
            ISiloStatusOracle siloStatusOracle,
            OrleansTaskScheduler scheduler,
            ActivationDirectory activationDirectory,
            ILoggerFactory loggerFactory)
        {
            this.loggerFactory = loggerFactory;
            this.siloStatusOracle = siloStatusOracle;
            this.scheduler = scheduler;
            this.activationDirectory = activationDirectory;
            this.consistentRingProvider = consistentRingProvider;
            this.runtimeClient = runtimeClient;
            this.ServiceId = clusterOptions.Value.ServiceId;
            this.SiloIdentity = siloDetails.SiloAddress.ToLongString();
            this.logger = this.loggerFactory.CreateLogger<SiloProviderRuntime>();
        }

        public SiloAddress ExecutingSiloAddress => this.siloStatusOracle.SiloAddress;

        public void RegisterSystemTarget(ISystemTarget target)
        {
            var systemTarget = target as SystemTarget;
            if (systemTarget == null) throw new ArgumentException($"Parameter must be of type {typeof(SystemTarget)}", nameof(target));
            systemTarget.RuntimeClient = this.runtimeClient;
            scheduler.RegisterWorkContext(systemTarget.SchedulingContext);
            activationDirectory.RecordNewSystemTarget(systemTarget);
        }

        public void UnregisterSystemTarget(ISystemTarget target)
        {
            var systemTarget = target as SystemTarget;
            if (systemTarget == null) throw new ArgumentException($"Parameter must be of type {typeof(SystemTarget)}", nameof(target));
            activationDirectory.RemoveSystemTarget(systemTarget);
            scheduler.UnregisterWorkContext(systemTarget.SchedulingContext);
        }

        public IConsistentRingProviderForGrains GetConsistentRingProvider(int mySubRangeIndex, int numSubRanges)
        {
            return new EquallyDividedRangeRingProvider(this.consistentRingProvider, this.loggerFactory, mySubRangeIndex, numSubRanges);
        }

        public async Task<IPersistentStreamPullingManager> InitializePullingAgents(
            string streamProviderName,
            IQueueAdapterFactory adapterFactory,
            IQueueAdapter queueAdapter)
        {
            IStreamQueueBalancer queueBalancer = CreateQueueBalancer(streamProviderName);
            var managerId = GrainId.NewSystemTargetGrainIdByTypeCode(Constants.PULLING_AGENTS_MANAGER_SYSTEM_TARGET_TYPE_CODE);
            var pullingAgentOptions = this.ServiceProvider.GetOptionsByName<StreamPullingAgentOptions>(streamProviderName);
            var subscriptionRegistrar = this.ServiceProvider.GetServiceByName<IStreamSubscriptionRegistrar<Guid, IStreamIdentity>>(streamProviderName)
                ?? this.ServiceProvider.GetService<IStreamSubscriptionRegistrar<Guid, IStreamIdentity>>();
            var manifest = this.ServiceProvider.GetServiceByName<IStreamSubscriptionManifest<Guid, IStreamIdentity>>(streamProviderName)
                ?? this.ServiceProvider.GetService<IStreamSubscriptionManifest<Guid, IStreamIdentity>>();
            var manager = new PersistentStreamPullingManager(managerId, streamProviderName, this, subscriptionRegistrar, manifest, adapterFactory, queueBalancer, pullingAgentOptions, this.loggerFactory);
            this.RegisterSystemTarget(manager);
            // Init the manager only after it was registered locally.
            var pullingAgentManager = manager.AsReference<IPersistentStreamPullingManager>();
            // Need to call it as a grain reference though.
            await pullingAgentManager.Initialize(queueAdapter.AsImmutable());
            return pullingAgentManager;
        }

        private IStreamQueueBalancer CreateQueueBalancer(string streamProviderName)
        {
            try
            {
                var balancer = this.ServiceProvider.GetServiceByName<IStreamQueueBalancer>(streamProviderName)??this.ServiceProvider.GetService<IStreamQueueBalancer>();
                if (balancer == null)
                    throw new ArgumentOutOfRangeException("balancerType", $"Cannot create stream queue balancer for StreamProvider: {streamProviderName}.Please configure your stream provider with a queue balancer.");
                this.logger.LogInformation($"Successfully created queue balancer of type {balancer.GetType()} for stream provider {streamProviderName}");
                return balancer;
            }
            catch (Exception e)
            {
                string error = $"Cannot create stream queue balancer for StreamProvider: {streamProviderName}, Exception: {e}. Please configure your stream provider with a queue balancer.";
                throw new ArgumentOutOfRangeException("balancerType", error);
            }
        }

        /// <inheritdoc />
        public string ExecutingEntityIdentity() => runtimeClient.CurrentActivationIdentity;

        /// <inheritdoc />
        public StreamDirectory GetStreamDirectory()
        {
            return runtimeClient.GetStreamDirectory();
        }

        /// <inheritdoc />
        public Task<Tuple<TExtension, TExtensionInterface>> BindExtension<TExtension, TExtensionInterface>(Func<TExtension> newExtensionFunc) where TExtension : IGrainExtension where TExtensionInterface : IGrainExtension
        {
            return runtimeClient.BindExtension<TExtension, TExtensionInterface>(newExtensionFunc);
        }
    }
}
