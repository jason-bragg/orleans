using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public class PubSubSubscriptionManifest : IStreamSubscriptionManifest<Guid,IStreamIdentity>
    {
        private readonly string name;
        IStreamProviderRuntime runtime;
        private readonly IStreamPubSub pubsub;

        private PubSubSubscriptionManifest(string name, IStreamProviderRuntime runtime, IStreamPubSub pubsub)
        {
            this.name = name;
            this.runtime = runtime;
            this.pubsub = pubsub;
        }

        public async Task<ChangeFeed<IList<StreamSubscription<Guid>>>> GetSubscriptionChanges(IStreamIdentity streamId)
        {
            Tuple<ProducerExtension, IStreamProducerExtension> extension = await runtime
                .BindExtension<ProducerExtension, IStreamProducerExtension>(() => new ProducerExtension(this.pubsub));
            var fullStreamId = StreamId.GetStreamId(streamId.Guid, this.name, streamId.Namespace);
            return await extension.Item1.Register(fullStreamId);
        }

        private class StreamSubscriptionPublisher : SharedStatePublisher<IList<StreamSubscription<Guid>>>
        {
            public StreamSubscriptionPublisher(IList<StreamSubscription<Guid>> initialState) : base(initialState) { }

            public ChangeFeed<IList<StreamSubscription<Guid>>> SubscriptionChangeFeed => base.Current;
        }

        private class ProducerExtension : IStreamProducerExtension
        {
            private readonly IStreamPubSub pubsub;
            private readonly Dictionary<StreamId, StreamSubscriptionPublisher> streamPublishers;

            public ProducerExtension(IStreamPubSub pubsub)
            {
                this.pubsub = pubsub;
                this.streamPublishers = new Dictionary<StreamId, StreamSubscriptionPublisher>();
            }

            public async Task<ChangeFeed<IList<StreamSubscription<Guid>>>> Register(StreamId streamId)
            {
                if(this.streamPublishers.TryGetValue(streamId, out StreamSubscriptionPublisher publisher))
                {
                    return publisher.SubscriptionChangeFeed;
                }
                ISet<PubSubSubscriptionState> pubsubStates = await this.pubsub.RegisterProducer(streamId, streamId.ProviderName, this);
                IList<StreamSubscription<Guid>> subscriptions = pubsubStates
                    .Select(state => new StreamSubscription<Guid>(state.SubscriptionId.Guid, state.consumerReference))
                    .ToList();
                this.streamPublishers[streamId] = publisher = new StreamSubscriptionPublisher(subscriptions);
                return publisher.SubscriptionChangeFeed;
            }

            public Task AddSubscriber(GuidId subscriptionId, StreamId streamId, IStreamConsumerExtension streamConsumer, IStreamFilterPredicateWrapper filter)
            {
                if (this.streamPublishers.TryGetValue(streamId, out StreamSubscriptionPublisher publisher))
                {
                    List<StreamSubscription<Guid>> update = new List<StreamSubscription<Guid>>(publisher.SubscriptionChangeFeed.Value);
                    update.Add(new StreamSubscription<Guid>(subscriptionId.Guid, (GrainReference)streamConsumer));
                    publisher.Publish(update);
                }
                return Task.CompletedTask;
            }

            public Task RemoveSubscriber(GuidId subscriptionId, StreamId streamId)
            {
                if (this.streamPublishers.TryGetValue(streamId, out StreamSubscriptionPublisher publisher))
                {
                    List<StreamSubscription<Guid>> update = publisher.SubscriptionChangeFeed.Value
                        .Where(s => s.SubscriptionId != subscriptionId.Guid)
                        .ToList();
                    publisher.Publish(update);
                }
                return Task.CompletedTask;
            }
        }

        public static IStreamSubscriptionManifest<Guid, IStreamIdentity> Create(IServiceProvider services, string name)
        {
            var runtime = services.GetService<IStreamProviderRuntime>();
            var pubsub = services.GetService<StreamPubSubImpl>();
            return new PubSubSubscriptionManifest(name, runtime, pubsub);
        }

        public static IStreamSubscriptionManifest<Guid, IStreamIdentity> CreateUsingImplicitOnly(IServiceProvider services, string name)
        {
            var runtime = services.GetService<IStreamProviderRuntime>();
            var pubsub = services.GetService<ImplicitStreamPubSub>();
            return new PubSubSubscriptionManifest(name, runtime, pubsub);
        }

        public static IStreamSubscriptionManifest<Guid, IStreamIdentity> CreateUsingExplicitOnly(IServiceProvider services, string name)
        {
            var runtime = services.GetService<IStreamProviderRuntime>();
            var pubsub = services.GetService<GrainBasedPubSubRuntime>();
            return new PubSubSubscriptionManifest(name, runtime, pubsub);
        }
    }
}
