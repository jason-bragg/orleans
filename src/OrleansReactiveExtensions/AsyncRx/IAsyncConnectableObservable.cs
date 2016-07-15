
using System.Threading.Tasks;

namespace Orleans.Rx.AsyncRx
{
    public interface IAsyncConnectableObservable<out T> : IAsyncObservable<T>
    {
        Task<IAsyncDisposable> Connect();
    }
}
