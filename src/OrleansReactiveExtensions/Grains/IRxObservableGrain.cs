

using System;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public interface IRxObservableGrain : IGrain
    {
        Task<IAsyncDisposable> SubscribeAsync(string topic, Guid subscriptionId, IRxObserverGrain observerGrain);
        Task UnsubscribeAsync(string topic, Guid subscriptionId);
    }
}
