using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Streams;
using Orleans.Serialization;
using Orleans.Configuration;

namespace Orleans.Providers.Streams.SimpleMessageStream
{
    public class SimpleMessageStreamProvider : IInternalStreamProvider, IStreamProvider
    {
        public string                       Name { get; private set; }

        private ILogger                      logger;
        private IStreamProviderRuntime      providerRuntime;
        private IRuntimeClient              runtimeClient;
        private ILoggerFactory              loggerFactory;
        private IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar;
        private IStreamSubscriptionManifest<Guid, IStreamIdentity> subscriptionManifest;
        private SerializationManager        serializationManager;
        private SimpleMessageStreamProviderOptions options;
        public bool IsRewindable { get { return false; } }

        public SimpleMessageStreamProvider(
            string name,
            SimpleMessageStreamProviderOptions options,
            IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar,
            IStreamSubscriptionManifest<Guid, IStreamIdentity> subscriptionManifest,
            ILoggerFactory loggerFactory,
            IProviderRuntime providerRuntime,
            SerializationManager serializationManager)
        {
            this.loggerFactory = loggerFactory;
            this.Name = name;
            this.logger = loggerFactory.CreateLogger($"{this.GetType().FullName}.{name}");
            this.options = options;
            this.providerRuntime = providerRuntime as IStreamProviderRuntime;
            this.runtimeClient = providerRuntime.ServiceProvider.GetService<IRuntimeClient>();
            this.serializationManager = serializationManager;
            this.subscriptionRegistrar = subscriptionRegistrar;
            this.subscriptionManifest = subscriptionManifest;
            logger.Info(
                "Initialized SimpleMessageStreamProvider with name {Name} and with property FireAndForgetDelivery: {FireAndForgetDelivery}, OptimizeForImmutableData: {OptimizeForImmutableData}", Name, this.options.FireAndForgetDelivery, this.options.OptimizeForImmutableData);
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
                this.options.FireAndForgetDelivery, this.options.OptimizeForImmutableData,
                this.subscriptionRegistrar, this.subscriptionManifest,
                IsRewindable, this.serializationManager, this.loggerFactory);
        }

        IInternalAsyncObservable<T> IInternalStreamProvider.GetConsumerInterface<T>(IAsyncStream<T> streamId)
        {
            return GetConsumerInterfaceImpl(streamId);
        }

        private IInternalAsyncObservable<T> GetConsumerInterfaceImpl<T>(IAsyncStream<T> stream)
        {
            return new StreamConsumer<T>((StreamImpl<T>)stream, Name, providerRuntime,
                this.subscriptionRegistrar, this.logger, IsRewindable);
        }

        public static IStreamProvider Create(IServiceProvider services, string name)
        {
            var subscriptionRegistrar = services.GetServiceByName<IStreamSubscriptionRegistrar<Guid, IStreamIdentity>>(name);
            var subscriptionManifest = services.GetServiceByName<IStreamSubscriptionManifest<Guid, IStreamIdentity>>(name);
            var options = services.GetService<IOptionsSnapshot<SimpleMessageStreamProviderOptions>>().Get(name);
            return ActivatorUtilities.CreateInstance<SimpleMessageStreamProvider>(services,
                name,
                subscriptionRegistrar,
                subscriptionManifest,
                options);
        }
    }
}
