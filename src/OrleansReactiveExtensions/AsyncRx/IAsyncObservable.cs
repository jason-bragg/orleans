
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public interface IAsyncObservable<out T>
    {
        Task<IAsyncDisposable> SubscribeAsync(IAsyncObserver<T> observer, CancellationToken token = default(CancellationToken));
    }
}
