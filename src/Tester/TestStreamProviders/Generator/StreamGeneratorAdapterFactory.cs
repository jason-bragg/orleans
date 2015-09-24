/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Tester.TestStreamProviders.Generator
{
    public abstract class StreamGeneratorAdapterFactory : QueueAdapterShellFactory
    {
        private readonly ComponentFactory factory;

        protected StreamGeneratorAdapterFactory(ComponentFactory factory)
        {
            this.factory = factory;
        }

        public interface IConfig : QueueAdapterShell.IConfig, IStreamGeneratorQueueConfig
        {
            int CacheSize { get; }
            int TotalQueueCount { get; }
        }

        public abstract IConfig BuildConfig(IProviderConfiguration config);

        protected override QueueFactoryShell CreateShell(IProviderConfiguration providerConfig, string providerName, Logger logger)
        {
            IConfig config = BuildConfig(providerConfig);
            IQueueAdapterCache queueAdapterCache = new SimpleQueueAdapterCache(config.CacheSize, logger);
            IStreamQueueMapper streamQueueMapper = new HashRingBasedStreamQueueMapper(config.TotalQueueCount, config.StreamProviderName);
            IQueueAdapterShellSender sender = new ReadOnlyQueueAdapterShellSender();
            IQueueAdapterShellReceiverFactory receiverFactory = new ReceiverFactory(factory, config, streamQueueMapper.GetAllQueues());;
            IQueueAdapter queueAdapter = new QueueAdapterShell(config, sender, receiverFactory);

            IFactory<QueueId, IStreamFailureHandler> streamFailureHandlerFactory = new FailureHandlerFactory(streamQueueMapper.GetAllQueues());

            return new QueueFactoryShell(queueAdapter, queueAdapterCache, streamQueueMapper, streamFailureHandlerFactory);
        }

        private class ReceiverFactory : IQueueAdapterShellReceiverFactory
        {
            private readonly QueueId[] queueIds;
            private readonly ConcurrentDictionary<QueueId, IStreamGenerator> queues;

            public ReceiverFactory(ComponentFactory factory, IStreamGeneratorQueueConfig generatorConfig, IEnumerable<QueueId> queueIds)
            {
                this.queueIds = queueIds.ToArray();
                queues = new ConcurrentDictionary<QueueId, IStreamGenerator>();
                foreach (QueueId queueId in this.queueIds)
                {
                    queues.TryAdd(queueId, factory.Create<IStreamGenerator>(generatorConfig.StreamGeneratorQueueTypeId, generatorConfig));
                }
            }

            public IQueueAdapterReceiver Create(QueueId queueId)
            {
                IStreamGenerator qeueue;
                if (!queues.TryGetValue(queueId, out qeueue))
                {
                    throw new ArgumentOutOfRangeException("queueId");
                }
                return new Receiver(queueId, qeueue);
            }
        }

        private class Receiver : IQueueAdapterReceiver
        {
            const int MaxDelayMs = 20;
            private readonly IStreamGenerator queue;
            private readonly Random random = new Random((int)DateTime.UtcNow.Ticks % int.MaxValue);

            public Receiver(QueueId queueId, IStreamGenerator queue)
            {
                Id = queueId;
                this.queue = queue;
            }

            public QueueId Id { get; private set; }

            public Task Initialize(TimeSpan timeout)
            {
                return TaskDone.Done;
            }

            public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
            {
                await Task.Delay(random.Next(MaxDelayMs));
                List<GeneratedBatchContainer> batches;
                if (!queue.TryReadEvents(DateTime.UtcNow, out batches))
                {
                    return new List<IBatchContainer>();
                }
                return batches.ToList<IBatchContainer>();
            }

            public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
            {
                return TaskDone.Done;
            }

            public Task Shutdown(TimeSpan timeout)
            {
                return TaskDone.Done;
            }
        }

        private class FailureHandlerFactory : Factory<QueueId, IStreamFailureHandler>
        {
            public FailureHandlerFactory(IEnumerable<QueueId> queueIds)
            {
                foreach (QueueId queueId in queueIds)
                {
                    // should log, but for now just ignore.
                    Register<NoOpStreamDeliveryFailureHandler>(queueId);
                }
            }
        }
    }
}
