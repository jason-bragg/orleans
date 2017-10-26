using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    internal class StreamSubscriptionHandle : IStreamSubscriptionHandle
    {
        private readonly IStreamPubSub streamPubSub;
        private readonly StreamConsumerExtension consumerExtension;

        public string ProviderName { get; }
        public IStreamIdentity StreamIdentity { get; }
        public GuidId SubscriptionId { get; }

        public StreamSubscriptionHandle(GuidId subscriptionId, string streamProviderName, IStreamIdentity streamId, IStreamPubSub streamPubSub, StreamConsumerExtension consumerExtension)
        {
            this.SubscriptionId = subscriptionId ?? throw new ArgumentNullException(nameof(subscriptionId));
            if (string.IsNullOrWhiteSpace(streamProviderName)) throw new ArgumentNullException(nameof(streamProviderName));
            this.ProviderName = streamProviderName;
            this.StreamIdentity = streamId ?? throw new ArgumentNullException(nameof(streamId));
            this.streamPubSub = streamPubSub ?? throw new ArgumentNullException(nameof(streamPubSub));
            this.consumerExtension = consumerExtension ?? throw new ArgumentNullException(nameof(consumerExtension));
        }

        public async Task UnsubscribeAsync()
        {
            consumerExtension.RemoveObserver(SubscriptionId);
            StreamId streamId = StreamId.GetStreamId(StreamIdentity.Guid, ProviderName, StreamIdentity.Namespace);
            await this.streamPubSub.UnregisterConsumer(SubscriptionId, streamId, ProviderName);
        }

        public Task<IStreamSubscriptionHandle> ResumeAsync<T>(IAsyncObserver<T> observer, StreamSequenceToken token = null)
        {
            this.consumerExtension.SetObserver<T>(this, observer, token);
            return Task.FromResult<IStreamSubscriptionHandle>(this);
        }

        public override string ToString()
        {
            return String.Format($"StreamSubscriptionHandle:Provider={this.ProviderName},Stream={this.StreamIdentity},SubscriptionId={this.SubscriptionId}");
        }
    }
}
