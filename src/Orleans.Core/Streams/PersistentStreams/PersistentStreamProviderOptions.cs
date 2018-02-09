
using Orleans.Streams;
using System;

namespace Orleans.Hosting
{
    public class PersistentStreamOptions
    {
        [Serializable]
        public enum RunState
        {
            None,
            Initialized,
            AgentsStarted,
            AgentsStopped,
        }

        public const string GET_QUEUE_MESSAGES_TIMER_PERIOD = "GetQueueMessagesTimerPeriod";
        public static readonly TimeSpan DEFAULT_GET_QUEUE_MESSAGES_TIMER_PERIOD = TimeSpan.FromMilliseconds(100);

        public const string INIT_QUEUE_TIMEOUT = "InitQueueTimeout";
        public static readonly TimeSpan DEFAULT_INIT_QUEUE_TIMEOUT = TimeSpan.FromSeconds(5);

        public const string MAX_EVENT_DELIVERY_TIME = "MaxEventDeliveryTime";
        public static readonly TimeSpan DEFAULT_MAX_EVENT_DELIVERY_TIME = TimeSpan.FromMinutes(1);

        public const string STREAM_INACTIVITY_PERIOD = "StreamInactivityPeriod";
        public static readonly TimeSpan DEFAULT_STREAM_INACTIVITY_PERIOD = TimeSpan.FromMinutes(30);

        public const string QUEUE_BALANCER_TYPE = "QueueBalancerType";
        //default balancer type if ConsistentRingBalancer
        public static Type DEFAULT_STREAM_QUEUE_BALANCER_TYPE = null;

        public const string STREAM_PUBSUB_TYPE = "PubSubType";
        public const StreamPubSubType DEFAULT_STREAM_PUBSUB_TYPE = StreamPubSubType.ExplicitGrainBasedAndImplicit;

        public const string SILO_MATURITY_PERIOD = "SiloMaturityPeriod";
        public static readonly TimeSpan DEFAULT_SILO_MATURITY_PERIOD = TimeSpan.FromMinutes(2);

        public TimeSpan GetQueueMsgsTimerPeriod { get; set; } = DEFAULT_GET_QUEUE_MESSAGES_TIMER_PERIOD;
        public TimeSpan InitQueueTimeout { get; set; } = DEFAULT_INIT_QUEUE_TIMEOUT;
        public TimeSpan MaxEventDeliveryTime { get; set; } = DEFAULT_MAX_EVENT_DELIVERY_TIME;
        public TimeSpan StreamInactivityPeriod { get; set; } = DEFAULT_STREAM_INACTIVITY_PERIOD;

        /// <summary>
        /// The queue balancer type for your stream provider. If you are using a custom queue balancer by injecting IStreamQueueBalancer as a transient service into DI,
        /// you should use your custom balancer's type
        /// </summary>
        public Type BalancerType { get; set; } = DEFAULT_STREAM_QUEUE_BALANCER_TYPE;
        public StreamPubSubType PubSubType { get; set; } = DEFAULT_STREAM_PUBSUB_TYPE;
        public TimeSpan SiloMaturityPeriod { get; set; } = DEFAULT_SILO_MATURITY_PERIOD;

        public RunState StartupState = StartupStateDefaultValue;
        public const RunState StartupStateDefaultValue = RunState.AgentsStarted;
    }
}
