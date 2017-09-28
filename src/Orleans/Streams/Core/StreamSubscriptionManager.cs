using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Core
{
    internal class StreamSubscriptionManager: IStreamSubscriptionManager
    {
        private readonly IStreamSubscriptionRegistrar subscriptionRegistrar;
        public StreamSubscriptionManager(IStreamSubscriptionRegistrar subscriptionRegistrar)
        {
            this.subscriptionRegistrar = subscriptionRegistrar;
        }

        public async Task<StreamSubscription> AddSubscription(string streamProviderName, IStreamIdentity streamIdentity, GrainReference grainRef)
        {
            var consumer = grainRef.AsReference<IStreamConsumerExtension>();
            var streamId = StreamId.GetStreamId(streamIdentity.Guid, streamProviderName, streamIdentity.Namespace);
            var subscriptionId = subscriptionRegistrar.CreateSubscriptionId(
                streamId, consumer);
            await subscriptionRegistrar.RegisterConsumer(subscriptionId, streamId, streamProviderName, consumer, null);
            var newSub = new StreamSubscription(subscriptionId.Guid, streamProviderName, streamId, grainRef.GrainId);
            return newSub;
        }

        public async Task RemoveSubscription(string streamProviderName, IStreamIdentity streamId, Guid subscriptionId)
        {
            await subscriptionRegistrar.UnregisterConsumer(GuidId.GetGuidId(subscriptionId), (StreamId)streamId, streamProviderName);
        }

        public Task<IEnumerable<StreamSubscription>> GetSubscriptions(string streamProviderName, IStreamIdentity streamIdentity)
        {
            var streamId = StreamId.GetStreamId(streamIdentity.Guid, streamProviderName, streamIdentity.Namespace);
            return subscriptionRegistrar.GetAllSubscriptions(streamId).ContinueWith(subs => subs.Result.AsEnumerable());
        }
    }

}

