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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;
using Tester.TestStreamProviders.Generator.Generators;

namespace Tester.TestStreamProviders.Generator
{
    public class GeneratorAdapterFactory : IQueueAdapterFactory, IQueueAdapter
    {
        private GeneratorAdapterConfig config;
        private Logger logger;

        public bool IsRewindable { get { return true; } }
        public StreamProviderDirection Direction { get { return StreamProviderDirection.ReadOnly; } }

        public void Init(IProviderConfiguration providerConfig, string providerName, Logger log)
        {
            this.logger = log;
            var generatorType = providerConfig.GetEnumProperty(GeneratorAdapterConfig.StreamGeneratorTypeIdName, GeneratorAdapterConfig.StreamGeneratorTypeIdDefault);
            switch (generatorType)
            {
                case StreamGeneratorsType.Simple:
                {
                    var cfg = new SimpleGeneratorAdapterConfig();
                    cfg.PopulateFromProviderConfig(providerConfig);
                    config = cfg;
                    break;
                }
                default:
                {
                    throw new ArgumentOutOfRangeException("providerConfig", "StreamGeneratorsType not set.");
                }
            }
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            return Task.FromResult<IQueueAdapter>(this);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return new SimpleQueueAdapterCache(config.CacheSize, logger);
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return new HashRingBasedStreamQueueMapper(config.TotalQueueCount, config.StreamProviderName);
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
        }

        public string Name { get { return config.StreamProviderName; } }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            return TaskDone.Done;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return new Receiver(queueId, BuildGenerator());
        }

        private class Receiver : IQueueAdapterReceiver
        {
            const int MaxDelayMs = 20;
            private readonly IStreamGenerator queue;
            private readonly Random random = new Random((int)DateTime.UtcNow.Ticks % int.MaxValue);

            public Receiver(QueueId queueId, IStreamGenerator queue)
            {
                if (queue == null)
                {
                    throw new NullReferenceException("queue");
                }
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
                await Task.Delay(random.Next(1,MaxDelayMs));
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

        private IStreamGenerator BuildGenerator()
        {
            switch (config.StreamGeneratorTypeId)
            {
                case StreamGeneratorsType.Simple:
                {
                    return new SimpleGenerator(config);
                }
            }
            return null;
        }
    }
}
