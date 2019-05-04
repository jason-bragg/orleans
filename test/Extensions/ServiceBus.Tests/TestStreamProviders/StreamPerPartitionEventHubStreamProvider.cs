using System;
using System.Text;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.ServiceBus.Providers;
using Orleans.Streams;
using TestExtensions;
using Xunit;
using Orleans;

namespace ServiceBus.Tests.TestStreamProviders.EventHub
{
    [Collection(TestEnvironmentFixture.DefaultCollection)]
    public class StreamPerPartitionEventHubStreamAdapterFactory : EventHubAdapterFactory
    {
        private StreamCacheEvictionOptions evictionOptions;
        public StreamPerPartitionEventHubStreamAdapterFactory(
            string name,
            EventHubOptions ehOptions,
            EventHubReceiverOptions receiverOptions,
            EventHubStreamCachePressureOptions cacheOptions,
            StreamCacheEvictionOptions evictionOptions,
            StreamStatisticOptions statisticOptions,
            IEventHubDataAdapter dataAdapter,
            IServiceProvider serviceProvider,
            SerializationManager serializationManager,
            ITelemetryProducer telemetryProducer,
            ILoggerFactory loggerFactory)
            : base(name, ehOptions, receiverOptions, cacheOptions, evictionOptions, statisticOptions, dataAdapter, serviceProvider, serializationManager, telemetryProducer, loggerFactory)
        {
            this.evictionOptions = evictionOptions;
        }

        protected override IEventHubQueueCacheFactory CreateCacheFactory(EventHubStreamCachePressureOptions options)
        {
            return new CustomCacheFactory(this.Name, evictionOptions, base.dataAdapter, SerializationManager);
        }

        private class CachedDataAdapter : EventHubDataAdapter
        {
            private readonly Guid partitionStreamGuid;

            public CachedDataAdapter(string partitionKey, SerializationManager serializationManager)
                : base(serializationManager)
            {
                partitionStreamGuid = GetPartitionGuid(partitionKey);
            }

            public override StreamPosition GetStreamPosition(EventData queueMessage)
            {
                IStreamIdentity stremIdentity = new StreamIdentity(partitionStreamGuid, null);
                StreamSequenceToken token =
                new EventHubSequenceTokenV2(queueMessage.SystemProperties.Offset, queueMessage.SystemProperties.SequenceNumber, 0);

                return new StreamPosition(stremIdentity, token);
            }
        }

        public static Guid GetPartitionGuid(string partition)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(partition);
            Array.Resize(ref bytes, 10);
            return new Guid(partition.GetHashCode(), bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9]);
        }

        private class CustomCacheFactory : IEventHubQueueCacheFactory
        {
            private readonly string name;
            private readonly StreamCacheEvictionOptions options;
            private readonly IEventHubDataAdapter dataAdapter;
            private readonly SerializationManager serializationManager;
            private readonly TimePurgePredicate timePurgePredicate;

            public CustomCacheFactory(string name, StreamCacheEvictionOptions options, IEventHubDataAdapter dataAdapter, SerializationManager serializationManager)
            {
                this.name = name;
                this.options = options;
                this.dataAdapter = dataAdapter;
                this.serializationManager = serializationManager;
                timePurgePredicate = new TimePurgePredicate(options.DataMinTimeInCache, options.DataMaxAgeInCache);
            }

            public IEventHubQueueCache CreateCache(string partition, IStreamQueueCheckpointer<string> checkpointer, ILoggerFactory loggerFactory, ITelemetryProducer telemetryProducer)
            {
                var bufferPool = new ObjectPool<FixedSizeBuffer>(() => new FixedSizeBuffer(1 << 20), null, null);
                var cacheLogger = loggerFactory.CreateLogger($"{typeof(EventHubQueueCache).FullName}.{this.name}.{partition}");
                var evictionStrategy = new ChronologicalEvictionStrategy(cacheLogger, this.timePurgePredicate, null, null);
                return new EventHubQueueCache(EventHubAdapterReceiver.MaxMessagesPerRead, bufferPool, this.dataAdapter, evictionStrategy, checkpointer, cacheLogger, null, null);
            }
        }

        public static new StreamPerPartitionEventHubStreamAdapterFactory Create(IServiceProvider services, string name)
        {
            var ehOptions = services.GetOptionsByName<EventHubOptions>(name);
            var receiverOptions = services.GetOptionsByName<EventHubReceiverOptions>(name);
            var cacheOptions = services.GetOptionsByName<EventHubStreamCachePressureOptions>(name);
            var statisticOptions = services.GetOptionsByName<StreamStatisticOptions>(name);
            var evictionOptions = services.GetOptionsByName<StreamCacheEvictionOptions>(name);
            var dataAdapter = services.GetServiceByName<IEventHubDataAdapter>(name)
                ?? services.GetRequiredService<IEventHubDataAdapter>()
                ?? ActivatorUtilities.CreateInstance<EventHubDataAdapter>(services);
            var factory = ActivatorUtilities.CreateInstance<StreamPerPartitionEventHubStreamAdapterFactory>(services, name, ehOptions, receiverOptions, cacheOptions, evictionOptions, statisticOptions, dataAdapter);
            factory.Init();
            return factory;
        }
    }
}