using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    internal interface IInternalAsyncObservable<T> : IAsyncObservable<T>
    {
        Task<IStreamSubscriptionHandle> ResumeAsync(
            IStreamSubscriptionHandle handle,
            IAsyncObserver<T> observer,
            StreamSequenceToken token = null);

        Task UnsubscribeAsync(IStreamSubscriptionHandle handle);

        Task<IList<IStreamSubscriptionHandle>> GetAllSubscriptions();

        Task Cleanup();
    }

        
    internal interface IInternalAsyncBatchObserver<in T> : IAsyncBatchObserver<T>
    {
        Task Cleanup();
    }
}
