
namespace Orleans.Rx
{
    public interface IAsyncSubject<in T, out R> : IAsyncObservable<R>, IAsyncObserver<T>
    {
    }

    public interface IAsyncSubject<T> : IAsyncSubject<T, T>
    {
    }
}
