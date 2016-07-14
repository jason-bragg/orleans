
using System;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public interface IRxObserverGrain : IGrain
    {
        Task OnNextAsync(string topic, Guid subscriptionId, object value);
        Task OnErrorAsync(string topic, Guid subscriptionId, Exception error);
        Task OnCompletedAsync(string topic, Guid subscriptionId);
    }
}
