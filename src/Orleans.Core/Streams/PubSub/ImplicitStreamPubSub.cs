using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    internal class ImplicitStreamPubSub : IStreamPubSub
    {
        private readonly IInternalGrainFactory grainFactory;
        private readonly ImplicitStreamSubscriberTable implicitTable;

        public ImplicitStreamPubSub(IInternalGrainFactory grainFactory, ImplicitStreamSubscriberTable implicitPubSubTable)
        {
            if (implicitPubSubTable == null)
            {
                throw new ArgumentNullException("implicitPubSubTable");
            }

            this.grainFactory = grainFactory;
            this.implicitTable = implicitPubSubTable;
        }

        public Task<ISet<PubSubSubscriptionState>> RegisterProducer(StreamId streamId, string streamProvider, IStreamProducerExtension streamProducer)
        {
            ISet<PubSubSubscriptionState> result = new HashSet<PubSubSubscriptionState>();
            if (!ImplicitStreamSubscriberTable.IsImplicitSubscribeEligibleNameSpace(streamId.Namespace)) return Task.FromResult(result);

            IDictionary<Guid, IStreamConsumerExtension> implicitSubscriptions = implicitTable.GetImplicitSubscribers(streamId, this.grainFactory);
            foreach (var kvp in implicitSubscriptions)
            {
                GuidId subscriptionId = GuidId.GetGuidId(kvp.Key);
                result.Add(new PubSubSubscriptionState(subscriptionId, streamId, kvp.Value));
            }
            return Task.FromResult(result);
        }

        public Task UnregisterProducer(StreamId streamId, string streamProvider, IStreamProducerExtension streamProducer)
        {
            return Task.CompletedTask;
        }

        public Task RegisterConsumer(GuidId subscriptionId, StreamId streamId, string streamProvider, IStreamConsumerExtension streamConsumer)
        {
            if (!IsImplicitSubscriber(streamConsumer, streamId))
            {
                throw new ArgumentOutOfRangeException(streamId.ToString(), "Only implicit subscriptions are supported.");
            }
            return Task.CompletedTask;
        }

        public Task UnregisterConsumer(GuidId subscriptionId, StreamId streamId, string streamProvider)
        {
            if (!IsImplicitSubscriber(subscriptionId, streamId))
            {
                throw new ArgumentOutOfRangeException(streamId.ToString(), "Only implicit subscriptions are supported.");
            }
            return Task.CompletedTask;
        }

        public Task<int> ProducerCount(Guid streamId, string streamProvider, string streamNamespace)
        {
            return Task.FromResult(0);
        }

        public Task<int> ConsumerCount(Guid streamId, string streamProvider, string streamNamespace)
        {
            return Task.FromResult(0);
        }

        public Task<List<StreamSubscription<Guid>>> GetAllSubscriptions(StreamId streamId, IStreamConsumerExtension streamConsumer = null)
        {
            if (!ImplicitStreamSubscriberTable.IsImplicitSubscribeEligibleNameSpace(streamId.Namespace))
                return Task.FromResult(new List<StreamSubscription<Guid>>());

            if (streamConsumer != null)
            {
                var subscriptionId = CreateSubscriptionId(streamId, streamConsumer);
                var grainRef = streamConsumer as GrainReference;
                return Task.FromResult(new List<StreamSubscription<Guid>>
                { new StreamSubscription<Guid>(subscriptionId.Guid, grainRef) });
            }
            else
            {
                var implicitConsumers = this.implicitTable.GetImplicitSubscribers(streamId, grainFactory);
                var subscriptions = implicitConsumers.Select(consumer =>
                {
                    var grainRef = consumer.Value as GrainReference;
                    var subId = consumer.Key;
                    return new StreamSubscription<Guid>(subId, grainRef);
                }).ToList();
                return Task.FromResult(subscriptions);
            }   
        }

        internal bool IsImplicitSubscriber(IAddressable addressable, StreamId streamId)
        {
            return implicitTable.IsImplicitSubscriber(GrainExtensions.GetGrainId(addressable), streamId);
        }

        internal bool IsImplicitSubscriber(GuidId subscriptionId, StreamId streamId)
        {
            return SubscriptionMarker.IsImplicitSubscription(subscriptionId.Guid);
        }

        public GuidId CreateSubscriptionId(StreamId streamId, IStreamConsumerExtension streamConsumer)
        {
            GrainId grainId = GrainExtensions.GetGrainId(streamConsumer);
            Guid subscriptionGuid;
            if (!implicitTable.TryGetImplicitSubscriptionGuid(grainId, streamId, out subscriptionGuid))
            {
                throw new ArgumentOutOfRangeException(streamId.ToString(), "Only implicit subscriptions are supported.");
            }
            return GuidId.GetGuidId(subscriptionGuid);
        }

        public Task<bool> FaultSubscription(StreamId streamId, GuidId subscriptionId)
        {
            return Task.FromResult(false);
        }
    }
}
