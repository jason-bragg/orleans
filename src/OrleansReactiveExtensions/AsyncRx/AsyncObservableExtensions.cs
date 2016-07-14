
using System;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public static class AsyncObservableExtensions
    {
        private static readonly Func<Exception, Task> DefaultOnError = _ => TaskDone.Done;
        private static readonly Func<Task> DefaultOnCompleted = () => TaskDone.Done;

        public static Task<IAsyncDisposable> SubscribeAsync<T>(this IAsyncObservable<T> obs,
                                                                           Func<T, Task> onNextAsync,
                                                                           Func<Exception, Task> onErrorAsync,
                                                                           Func<Task> onCompletedAsync)
        {
            var observer = new AsyncObserver<T>(onNextAsync, onErrorAsync, onCompletedAsync);
            return obs.SubscribeAsync(observer);
        }

        public static Task<IAsyncDisposable> SubscribeAsync<T>(this IAsyncObservable<T> obs,
                                                                           Func<T, Task> onNextAsync,
                                                                           Func<Exception, Task> onErrorAsync)
        {
            return obs.SubscribeAsync(onNextAsync, onErrorAsync, DefaultOnCompleted);
        }

        public static Task<IAsyncDisposable> SubscribeAsync<T>(this IAsyncObservable<T> obs,
                                                                           Func<T, Task> onNextAsync,
                                                                           Func<Task> onCompletedAsync)
        {
            return obs.SubscribeAsync(onNextAsync, DefaultOnError, onCompletedAsync);
        }

        public static Task<IAsyncDisposable> SubscribeAsync<T>(this IAsyncObservable<T> obs,
                                                                           Func<T, Task> onNextAsync)
        {
            return obs.SubscribeAsync(onNextAsync, DefaultOnError, DefaultOnCompleted);
        }

        public static Task<IAsyncDisposable> ToSynchronous<T>(this IAsyncObservable<T> obs, Func<IObservable<T>, IDisposable> rxAction)
        {
            var subject = new Subject<T>();
            var disposer = rxAction(subject);
            var container = Tuple.Create(subject, disposer);
            return obs.SubscribeAsync(
                e => SynchronousAction(() => container.Item1.OnNext(e)),
                ex => SynchronousAction(() => container.Item1.OnError(ex)),
                () => SynchronousAction(() => container.Item1.OnCompleted()));
        }

        private static Task SynchronousAction(Action action)
        {
            action();
            return TaskDone.Done;
        }

        private class AsyncObserver<T> : IAsyncObserver<T>
        {
            private readonly Func<T, Task> onNextAsync;
            private readonly Func<Exception, Task> onErrorAsync;
            private readonly Func<Task> onCompletedAsync;
            private bool isActive;

            public AsyncObserver(Func<T, Task> onNextAsync, Func<Exception, Task> onErrorAsync, Func<Task> onCompletedAsync)
            {
                if (onNextAsync == null) throw new ArgumentNullException("onNextAsync");
                if (onErrorAsync == null) throw new ArgumentNullException("onErrorAsync");
                if (onCompletedAsync == null) throw new ArgumentNullException("onCompletedAsync");

                this.onNextAsync = onNextAsync;
                this.onErrorAsync = onErrorAsync;
                this.onCompletedAsync = onCompletedAsync;
                isActive = true;
            }

            public Task OnNextAsync(T item, CancellationToken token = default(CancellationToken))
            {
                return isActive ? onNextAsync(item) : TaskDone.Done;
            }

            public Task OnCompletedAsync(CancellationToken token = default(CancellationToken))
            {
                var task = isActive ? onCompletedAsync() : TaskDone.Done;
                isActive = false;
                return task;
            }

            public Task OnErrorAsync(Exception ex, CancellationToken token = default(CancellationToken))
            {
                var task = isActive ? onErrorAsync(ex) : TaskDone.Done;
                isActive = false;
                return task;
            }

            public Task DisposeAsync(CancellationToken token = new CancellationToken())
            {
                throw new InvalidOperationException($"{GetType()} should never be disposed");
            }
        }
    }
}
