using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Common
{
    [Serializable]
    public enum PersistentStreamProviderCommand
    {
        None,
        StartAgents,
        StopAgents,
        GetAgentsState,
        GetNumberRunningAgents,
        AdapterCommandStartRange = 10000,
        AdapterCommandEndRange = AdapterCommandStartRange + 9999,
        AdapterFactoryCommandStartRange = AdapterCommandEndRange + 1,
        AdapterFactoryCommandEndRange = AdapterFactoryCommandStartRange + 9999,
    }

    /// <summary>
    /// Persistent stream provider that uses an adapter for persistence
    /// </summary>
    public class PersistentStreamProvider : IStreamProvider, IInternalStreamProvider, IControllable, ILifecycleParticipant<ILifecycleObservable>
    {
        private readonly ILogger logger;
        private readonly IStreamProviderRuntime runtime;
        private readonly SerializationManager serializationManager;
        private readonly IRuntimeClient runtimeClient;
        private readonly ProviderStateManager stateManager = new ProviderStateManager();
        private IQueueAdapterFactory    adapterFactory;
        private IQueueAdapter           queueAdapter;
        private IPersistentStreamPullingManager pullingAgentManager;
        private readonly StreamLifecycleOptions lifeCycleOptions;
        private readonly IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar;
        private readonly IStreamSubscriptionManifest<Guid, IStreamIdentity> subscriptionManifest;

        public string Name { get; private set; }
        public bool IsRewindable { get { return queueAdapter.IsRewindable; } }

        public PersistentStreamProvider(
            string name,
            StreamLifecycleOptions lifeCycleOptions,
            IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar,
            IStreamSubscriptionManifest<Guid, IStreamIdentity> subscriptionManifest,
            IProviderRuntime runtime,
            SerializationManager serializationManager,
            ILogger<PersistentStreamProvider> logger)
        {
            if (String.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            if (runtime == null) throw new ArgumentNullException(nameof(runtime));
            this.Name = name;
            this.subscriptionRegistrar = subscriptionRegistrar;
            this.subscriptionManifest = subscriptionManifest;
            this.lifeCycleOptions = lifeCycleOptions ?? throw new ArgumentNullException(nameof(lifeCycleOptions));
            this.runtime = runtime.ServiceProvider.GetRequiredService<IStreamProviderRuntime>();
            this.runtimeClient = runtime.ServiceProvider.GetRequiredService<IRuntimeClient>();
            this.serializationManager = serializationManager ?? throw new ArgumentNullException(nameof(serializationManager));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        private async Task Init(CancellationToken token) 
        {
            if(!this.stateManager.PresetState(ProviderState.Initialized)) return;
            this.adapterFactory = this.runtime.ServiceProvider.GetRequiredServiceByName<IQueueAdapterFactory>(this.Name);
            this.queueAdapter = await adapterFactory.CreateAdapter();
            this.stateManager.CommitState();
        }

        private async Task Start(CancellationToken token)
        {
            if (!this.stateManager.PresetState(ProviderState.Started)) return;
            if (this.queueAdapter.Direction.Equals(StreamProviderDirection.ReadOnly) ||
                this.queueAdapter.Direction.Equals(StreamProviderDirection.ReadWrite))
            {
                var siloRuntime = this.runtime as ISiloSideStreamProviderRuntime;
                if (siloRuntime != null)
                {
                    this.pullingAgentManager = await siloRuntime.InitializePullingAgents(this.Name, this.adapterFactory, this.queueAdapter);

                    // TODO: No support yet for DeliveryDisabled, only Stopped and Started
                    if (this.lifeCycleOptions.StartupState == StreamLifecycleOptions.RunState.AgentsStarted)
                        await pullingAgentManager.StartAgents();
                }
            }
            stateManager.CommitState();
        }

        private async Task Close(CancellationToken token)
        {
            if (!stateManager.PresetState(ProviderState.Closed)) return;
            var siloRuntime = this.runtime as ISiloSideStreamProviderRuntime;
            if (siloRuntime != null)
            {
                await pullingAgentManager.Stop();
            }
            stateManager.CommitState();
        }

        public IAsyncStream<T> GetStream<T>(Guid id, string streamNamespace)
        {
            var streamId = StreamId.GetStreamId(id, Name, streamNamespace);
            return this.runtime.GetStreamDirectory().GetOrAddStream<T>(
                streamId, () => new StreamImpl<T>(streamId, this, IsRewindable, this.runtimeClient));
        }

        IInternalAsyncBatchObserver<T> IInternalStreamProvider.GetProducerInterface<T>(IAsyncStream<T> stream)
        {
            if (queueAdapter.Direction == StreamProviderDirection.ReadOnly)
            {
                throw new InvalidOperationException($"Stream provider {queueAdapter.Name} is ReadOnly.");
            }
            return new PersistentStreamProducer<T>((StreamImpl<T>)stream, this.runtime, queueAdapter, IsRewindable, this.serializationManager);
        }

        IInternalAsyncObservable<T> IInternalStreamProvider.GetConsumerInterface<T>(IAsyncStream<T> streamId)
        {
            return GetConsumerInterfaceImpl(streamId);
        }

        private IInternalAsyncObservable<T> GetConsumerInterfaceImpl<T>(IAsyncStream<T> stream)
        {
            return new StreamConsumer<T>((StreamImpl<T>)stream, Name, this.runtime, this.subscriptionRegistrar, this.logger, IsRewindable);
        }

        public Task<object> ExecuteCommand(int command, object arg)
        {
            if (command >= (int)PersistentStreamProviderCommand.AdapterCommandStartRange &&
                command <= (int)PersistentStreamProviderCommand.AdapterCommandEndRange &&
                queueAdapter is IControllable)
            {
                return ((IControllable)queueAdapter).ExecuteCommand(command, arg);
            }

            if (command >= (int)PersistentStreamProviderCommand.AdapterFactoryCommandStartRange &&
                command <= (int)PersistentStreamProviderCommand.AdapterFactoryCommandEndRange &&
                adapterFactory is IControllable)
            {
                return ((IControllable)adapterFactory).ExecuteCommand(command, arg);
            }
            
            if (pullingAgentManager != null)
            {
                return pullingAgentManager.ExecuteCommand((PersistentStreamProviderCommand)command, arg);
            }

            logger.Warn(0, $"Got command {(PersistentStreamProviderCommand)command} with arg {arg}, but PullingAgentManager is not initialized yet. Ignoring the command.");
            throw new ArgumentException("PullingAgentManager is not initialized yet.");
        }

        public void Participate(ILifecycleObservable lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<PersistentStreamProvider>(this.Name), this.lifeCycleOptions.InitStage, Init);
            lifecycle.Subscribe(OptionFormattingUtilities.Name<PersistentStreamProvider>(this.Name), this.lifeCycleOptions.StartStage, Start, Close);
        }

        public static IStreamProvider Create(IServiceProvider services, string name)
        {
            var initOptions = services.GetRequiredService<IOptionsSnapshot<StreamLifecycleOptions>>().Get(name);
            var subscriptionRegistrar = services.GetServiceByName<IStreamSubscriptionRegistrar<Guid, IStreamIdentity>>(name);
            var subscriptionManifest = services.GetServiceByName<IStreamSubscriptionManifest<Guid, IStreamIdentity>>(name);

            return ActivatorUtilities.CreateInstance<PersistentStreamProvider>(services, name, initOptions, subscriptionRegistrar, subscriptionManifest);
        }

        public static ILifecycleParticipant<TLifecycle> ParticipateIn<TLifecycle>(IServiceProvider serviceProvider, string name)
            where TLifecycle : ILifecycleObservable
        {
            var provider = (PersistentStreamProvider)serviceProvider.GetRequiredServiceByName<IStreamProvider>(name);
            return provider.ParticipateIn<TLifecycle>();
        }
    }
}
