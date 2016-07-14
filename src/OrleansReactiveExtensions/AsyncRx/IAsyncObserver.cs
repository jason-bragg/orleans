
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public interface IAsyncObserver<in T> : IAsyncDisposable
    {
        Task OnNextAsync(T value, CancellationToken token = default(CancellationToken));
        Task OnErrorAsync(Exception error, CancellationToken token = default(CancellationToken));
        Task OnCompletedAsync(CancellationToken token = default(CancellationToken));
    }
}
