using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Streams.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Serialization;

namespace Orleans.Providers.Streams.SimpleMessageStream
{
    public class SimpleMessageStreamProviderOptions
    {
        public const bool DEFAULT_VALUE_FIRE_AND_FORGET_DELIVERY = false;
        public const bool DEFAULT_VALUE_OPTIMIZE_FOR_IMMUTABLE_DATA = true;
        public bool FireAndForgetDelivery { get; set; } = DEFAULT_VALUE_FIRE_AND_FORGET_DELIVERY;
        public bool OptimizeForImmutableData { get; set; } = DEFAULT_VALUE_OPTIMIZE_FOR_IMMUTABLE_DATA;
        public StreamPubSubType PubSubType { get; set; } = DEFAULT_PUBSUB_TYPE;
        public static StreamPubSubType DEFAULT_PUBSUB_TYPE = StreamPubSubType.ExplicitGrainBasedAndImplicit;

        public int InitStage { get; set; } = DEFAULT_INIT_STAGE;
        //TODO remove the hard coded stage after Jason rebase with master
        public const int DEFAULT_INIT_STAGE = 5000;
    }

    public class SimpleMessageStreamProvider : IInternalStreamProvider, IStreamProvider, IStreamSubscriptionManagerRetriever, ILifecycleParticipant<ILifecycleObservable>
    {
        public string                       Name { get; private set; }

        private ILogger                      logger;
        private IStreamProviderRuntime      providerRuntime;
        private IRuntimeClient              runtimeClient;
        private IStreamSubscriptionManager  streamSubscriptionManager;
        private ILoggerFactory              loggerFactory;
        private SerializationManager        serializationManager;
        private SimpleMessageStreamProviderOptions options;
        public bool IsRewindable { get { return false; } }

        public SimpleMessageStreamProvider(string name, SimpleMessageStreamProviderOptions options,
            ILoggerFactory loggerFactory, IProviderRuntime providerRuntime, SerializationManager serializationManager)
        {
            this.loggerFactory = loggerFactory;
            this.Name = name;
            this.logger = loggerFactory.CreateLogger($"{this.GetType().FullName}.{name}");
            this.options = options;
            this.providerRuntime = providerRuntime as IStreamProviderRuntime;
            this.runtimeClient = providerRuntime.ServiceProvider.GetService<IRuntimeClient>();
            this.serializationManager = serializationManager;
            if (this.options.PubSubType == StreamPubSubType.ExplicitGrainBasedAndImplicit
                || this.options.PubSubType == StreamPubSubType.ExplicitGrainBasedOnly)
            {
                this.streamSubscriptionManager = this.providerRuntime.ServiceProvider
                    .GetService<IStreamSubscriptionManagerAdmin>()
                    .GetStreamSubscriptionManager(StreamSubscriptionManagerType.ExplicitSubscribeOnly);
            }
            logger.Info(
                "Initialized SimpleMessageStreamProvider with name {0} and with property FireAndForgetDelivery: {1}, OptimizeForImmutableData: {2} " +
                "and PubSubType: {3}", Name, this.options.FireAndForgetDelivery, this.options.OptimizeForImmutableData,
                this.options.PubSubType);
        }

        public IStreamSubscriptionManager GetStreamSubscriptionManager()
        {
            return this.streamSubscriptionManager;
        }

        public IAsyncStream<T> GetStream<T>(Guid id, string streamNamespace)
        {
            var streamId = StreamId.GetStreamId(id, Name, streamNamespace);
            return providerRuntime.GetStreamDirectory().GetOrAddStream<T>(
                streamId,
                () => new StreamImpl<T>(streamId, this, IsRewindable, this.runtimeClient));
        }

        IInternalAsyncBatchObserver<T> IInternalStreamProvider.GetProducerInterface<T>(IAsyncStream<T> stream)
        {
            return new SimpleMessageStreamProducer<T>((StreamImpl<T>)stream, Name, providerRuntime,
                this.options.FireAndForgetDelivery, this.options.OptimizeForImmutableData, providerRuntime.PubSub(this.options.PubSubType), IsRewindable,
                this.serializationManager, this.loggerFactory);
        }

        IInternalAsyncObservable<T> IInternalStreamProvider.GetConsumerInterface<T>(IAsyncStream<T> streamId)
        {
            return GetConsumerInterfaceImpl(streamId);
        }

        private IInternalAsyncObservable<T> GetConsumerInterfaceImpl<T>(IAsyncStream<T> stream)
        {
            return new StreamConsumer<T>((StreamImpl<T>)stream, Name, providerRuntime,
                providerRuntime.PubSub(this.options.PubSubType), this.logger, IsRewindable);
        }

        public void Participate(ILifecycleObservable lifecycle)
        {
            //just need a dumb method to make DI resolve this,  hence finish the initialization
            return;
        }

        public static IStreamProvider Create(IServiceProvider services, string name)
        {
            return ActivatorUtilities.CreateInstance<SimpleMessageStreamProvider>(services, services.GetService<IOptionsSnapshot<SimpleMessageStreamProviderOptions>>().Get(name), name);
        }
    }
}
