using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Streams
{
    internal class StreamConsumerCollection
    {
        private readonly Dictionary<Guid, StreamConsumerData> queueData; // map of consumers for one stream: from Guid ConsumerId to StreamConsumerData
        private DateTime lastActivityTimeUtc;

        public StreamId StreamId { get; }
        public ChangeFeed<IList<StreamSubscription<Guid>>> SubscriptionChangeFeed { get; set; }
        public bool StreamRegistered => this.SubscriptionChangeFeed != null;

        public StreamConsumerCollection(StreamId streamId, DateTime nowUtc)
        {
            this.StreamId = streamId;
            this.lastActivityTimeUtc = nowUtc;
            queueData = new Dictionary<Guid, StreamConsumerData>();
        }

        public StreamConsumerData AddConsumer(Guid subscriptionId, IStreamIdentity streamId, IStreamConsumerExtension streamConsumer, DateTime nowUtc)
        {
            var consumerData = new StreamConsumerData(subscriptionId, streamId, streamConsumer);
            queueData.Add(subscriptionId, consumerData);
            this.lastActivityTimeUtc = nowUtc;
            return consumerData;
        }

        public bool RemoveConsumer(Guid subscriptionId, ILogger logger)
        {
            StreamConsumerData consumer;
            if (!queueData.TryGetValue(subscriptionId, out consumer)) return false;

            consumer.SafeDisposeCursor(logger);
            return queueData.Remove(subscriptionId);
        }

        public bool TryGetConsumer(Guid subscriptionId, out StreamConsumerData data)
        {
            return queueData.TryGetValue(subscriptionId, out data);
        }

        public IEnumerable<StreamConsumerData> AllConsumers()
        {
            return queueData.Values;
        }

        public void DisposeAll(ILogger logger)
        {
            foreach (StreamConsumerData consumer in queueData.Values)
            {
                consumer.SafeDisposeCursor(logger);
            }
            this.SubscriptionChangeFeed?.Dispose();
            queueData.Clear();
        }


        public int Count
        {
            get { return queueData.Count; }
        }

        public void RefreshActivity(DateTime nowUtc)
        {
            this.lastActivityTimeUtc = nowUtc;
        }

        public bool IsInactive(DateTime nowUtc, TimeSpan inactivityPeriod)
        {
            // Consider stream inactive (with all its consumers) from the pulling agent perspective if:
            // 1) There were no new events received for that stream in the last inactivityPeriod
            // 2) All consumer for that stream are currently inactive (that is, all cursors are inactive) - 
            //    meaning there is nothing for those consumers in the adapter cache.
            if (nowUtc - this.lastActivityTimeUtc < inactivityPeriod) return false;
            return !queueData.Values.Any(data => data.State.Equals(StreamConsumerDataState.Active));
        }
    }
}
