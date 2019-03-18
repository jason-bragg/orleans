using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public interface IStreamSubscriptionManifest<TSubscriptionId, TStreamId>
    {
        Task<ChangeFeed<IList<StreamSubscription<TSubscriptionId>>>> GetSubscriptionChanges(TStreamId streamId);
    }
}
