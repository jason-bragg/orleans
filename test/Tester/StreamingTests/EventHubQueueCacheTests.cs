
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.ServiceBus.Providers;
using Orleans.Streams;
using Orleans.TestingHost.Utils;
using Xunit;

namespace Tester.StreamingTests
{
    public class EventHubQueueCacheTests
    {
        private static readonly byte[] Payload = Encoding.UTF8.GetBytes("blarg");
        const string StreamNamespace = "StreamNamespace";

        public EventHubQueueCacheTests()
        {
            SerializationManager.InitializeForTesting();
        }

        /// <summary>
        /// We only caluclate backpressure if we have more than 10 times the max add count already loaded into the cache.
        /// </summary>
        [Fact, TestCategory("Functional")]
        public void NoBackpressureUntilSufficientDataTest()
        {
            IStreamIdentity[] streamIds =
            {
                new StreamIdentity(Guid.NewGuid(), StreamNamespace),
                new StreamIdentity(Guid.NewGuid(), StreamNamespace)
            };
 
            var bufferPool = new FixedSizeObjectPool<FixedSizeBuffer>(128, pool => new FixedSizeBuffer(1 << 20, pool));
            IEventHubQueueCache cache = new EventHubQueueCache(TestNoOpCheckpointer.Instance, bufferPool, NoOpTestLogger.Instance);
            int maxAddCount = cache.GetMaxAddCount();
            int sufficientData = maxAddCount * 10;
            object cursor = null;
            IStreamIdentity streamId;
            IBatchContainer message;
            for (int i = 0; i < sufficientData; i++)
            {
                streamId = streamIds[i%streamIds.Length];
                cache.Add(MakeEventData(streamId, i, Payload), DateTime.UtcNow + TimeSpan.FromSeconds(1));
                if (i == 0)
                {
                    // get cursor to first item in cache
                    cursor = cache.GetCursor(streamIds.First(), null);
                }
            }
            // cursor is in back of cache, so cache should be under pressure after read, if we'd read enough data, but we haven't
            Assert.True(cache.TryGetNextMessage(cursor, out message));
            Assert.Equal(maxAddCount, cache.GetMaxAddCount());

            // add one more, we should now have enough to start using backpressure
            int sequenceNumber = sufficientData;
            streamId = streamIds[sequenceNumber % streamIds.Length];
            cache.Add(MakeEventData(streamId, sequenceNumber, Payload), DateTime.UtcNow + TimeSpan.FromSeconds(1));

            // cursor is in back of cache, so cache should be under pressure after read
            Assert.True(cache.TryGetNextMessage(cursor, out message));
            Assert.Equal(0, cache.GetMaxAddCount());
        }

        private EventData MakeEventData(IStreamIdentity streamId, long sequenceNumber, byte[] payload)
        {
            var data = new EventData(payload) {PartitionKey = $"{streamId.Guid}"};
            data.Properties["StreamNamespace"] = streamId.Namespace;
            data.SystemProperties["Offset"] = $"{sequenceNumber}";
            data.SystemProperties["SequenceNumber"] = sequenceNumber;
            data.SystemProperties["EnqueuedTimeUtc"] = DateTime.UtcNow;
            return data;
        }

        private class TestNoOpCheckpointer : IStreamQueueCheckpointer<string>
        {
            public static readonly IStreamQueueCheckpointer<string> Instance = new TestNoOpCheckpointer();

            public bool CheckpointExists => true;

            public Task<string> Load()
            {
                return Task.FromResult("-1");
            }

            public void Update(string offset, DateTime utcNow)
            {
            }
        }
    }
}
