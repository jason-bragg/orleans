using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public interface IStreamSubscriptionRegistrar<TSubscriptionId, TStreamId>
    {
        TSubscriptionId CreateSubscription(TStreamId streamId, GrainReference consumerReference);
        Task Register(TStreamId streamId, TSubscriptionId subscriptionId, GrainReference consumerReference);
        Task Unregister(TStreamId streamId, TSubscriptionId subscriptionId);
        Task<IList<StreamSubscription<TSubscriptionId>>> GetConsumerSubscriptions(TStreamId streamId, GrainReference consumer);
    }
}
