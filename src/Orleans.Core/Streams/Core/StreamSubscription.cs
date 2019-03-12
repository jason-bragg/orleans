using System;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Streams
{
    [Serializable]
    [Immutable]
    public class StreamSubscription<TSubscriptionId>
    {
        public StreamSubscription(TSubscriptionId subscriptionId, GrainReference consumer)
        {
            this.SubscriptionId = subscriptionId;
            this.Consumer = consumer;
        }
        public TSubscriptionId SubscriptionId { get; }
        public GrainReference Consumer { get; }
    }
}
