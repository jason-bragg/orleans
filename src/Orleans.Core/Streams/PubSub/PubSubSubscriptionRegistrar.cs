using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public class PubSubSubscriptionRegistrar : IStreamSubscriptionRegistrar<Guid, IStreamIdentity>
    {
        private readonly string name;
        private readonly IStreamPubSub pubsub;

        private PubSubSubscriptionRegistrar(string name, IStreamPubSub pubsub)
        {
            this.name = name;
            this.pubsub = pubsub;
        }

        public Guid CreateSubscription(IStreamIdentity streamId, GrainReference consumerReference)
        {
            var fullStreamId = StreamId.GetStreamId(streamId.Guid, this.name, streamId.Namespace);
            var consumerExtension = consumerReference.AsReference<IStreamConsumerExtension>();
            GuidId subscriptionId = this.pubsub.CreateSubscriptionId(fullStreamId, consumerExtension);
            return subscriptionId.Guid;
        }

        public Task<IList<StreamSubscription<Guid>>> GetConsumerSubscriptions(IStreamIdentity streamId, GrainReference consumerReference)
        {
            var fullStreamId = StreamId.GetStreamId(streamId.Guid, this.name, streamId.Namespace);
            var consumerExtension = consumerReference.AsReference<IStreamConsumerExtension>();
            return this.pubsub.GetAllSubscriptions(fullStreamId, consumerExtension);
        }

        public Task Register(IStreamIdentity streamId, Guid subscriptionId, GrainReference consumerReference)
        {
            GuidId guidId = GuidId.GetGuidId(subscriptionId);
            var fullStreamId = StreamId.GetStreamId(streamId.Guid, this.name, streamId.Namespace);
            var consumerExtension = consumerReference.AsReference<IStreamConsumerExtension>();
            return this.pubsub.RegisterConsumer(guidId, fullStreamId, this.name, consumerExtension);
        }

        public Task Unregister(IStreamIdentity streamId, Guid subscriptionId)
        {
            GuidId guidId = GuidId.GetGuidId(subscriptionId);
            var fullStreamId = StreamId.GetStreamId(streamId.Guid, this.name, streamId.Namespace);
            return this.pubsub.UnregisterConsumer(guidId, fullStreamId, this.name);
        }

        public static IStreamSubscriptionRegistrar<Guid, IStreamIdentity>  Create(IServiceProvider services, string name)
        {
            var pubsub = services.GetService<StreamPubSubImpl>();
            return new PubSubSubscriptionRegistrar(name, pubsub);
        }

        public static IStreamSubscriptionRegistrar<Guid, IStreamIdentity> CreateUsingImplicitOnly(IServiceProvider services, string name)
        {
            var pubsub = services.GetService<ImplicitStreamPubSub>();
            return new PubSubSubscriptionRegistrar(name, pubsub);
        }

        public static IStreamSubscriptionRegistrar<Guid, IStreamIdentity> CreateUsingExplicitOnly(IServiceProvider services, string name)
        {
            var pubsub = services.GetService<GrainBasedPubSubRuntime>();
            return new PubSubSubscriptionRegistrar(name, pubsub);
        }
    }
}
