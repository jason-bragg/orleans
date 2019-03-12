using Orleans.Runtime;

namespace Orleans.Streams
{
    public interface IStreamSubscriptionHandleFactory
    {
        IStreamIdentity StreamId { get; }
        string ProviderName { get; }
        GuidId SubscriptionId { get; }
        StreamSubscriptionHandle<T> Create<T>();
    }
}
