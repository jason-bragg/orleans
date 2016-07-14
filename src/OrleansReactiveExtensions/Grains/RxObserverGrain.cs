
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public interface IDisposableAsyncObservable<out T> : IAsyncObservable<T>, IAsyncDisposable
    {
    }

    public class RxObserverGrain : Grain, IRxObserverGrain
    {
        private Dictionary<Tuple<string, Guid>, IObserverBridge> bridges;

        public override Task OnActivateAsync()
        {
            bridges = new Dictionary<Tuple<string, Guid>, IObserverBridge>();
            return base.OnActivateAsync();
        }

        public override async Task OnDeactivateAsync()
        {
            List<Task> pending = bridges.Values.Select(bridge => bridge.DisposeAsync()).ToList();
            await Task.WhenAll(pending);
            await base.OnDeactivateAsync();
        }

        public async Task<IDisposableAsyncObservable<T>> Subscribe<T>(IRxObservableGrain grain, string topic)
        {
            Guid subscriptionId = Guid.NewGuid();
            IAsyncDisposable disposer = await grain.SubscribeAsync(topic, subscriptionId, this.AsReference<IRxObserverGrain>());
            var bridge = new ObserverBridge<T>(disposer);
            bridges.Add(Tuple.Create(topic, subscriptionId), bridge);
            return bridge;
        }

        public Task OnNextAsync(string topic, Guid subscriptionId, object value)
        {
            Tuple<string, Guid> bridgeId = Tuple.Create(topic, subscriptionId);
            IObserverBridge bridge;
            if (!bridges.TryGetValue(bridgeId, out bridge))
            {
                throw new ArgumentOutOfRangeException($"Subscription {subscriptionId} was not found on topic {topic}.");
            }
            return bridge.OnNextAsync(value);
        }

        public Task OnErrorAsync(string topic, Guid subscriptionId, Exception error)
        {
            Tuple<string, Guid> bridgeId = Tuple.Create(topic, subscriptionId);
            IObserverBridge bridge;
            if (!bridges.TryGetValue(bridgeId, out bridge))
            {
                throw new ArgumentOutOfRangeException($"Subscription {subscriptionId} was not found on topic {topic}.");
            }
            return bridge.OnErrorAsync(error);
        }

        public Task OnCompletedAsync(string topic, Guid subscriptionId)
        {
            Tuple<string, Guid> bridgeId = Tuple.Create(topic, subscriptionId);
            IObserverBridge bridge;
            if (!bridges.TryGetValue(bridgeId, out bridge))
            {
                throw new ArgumentOutOfRangeException($"Subscription {subscriptionId} was not found on topic {topic}.");
            }
            return bridge.OnCompletedAsync();
        }

        private interface IObserverBridge : IAsyncDisposable
        {
            Task OnErrorAsync(Exception error, CancellationToken token = new CancellationToken());
            Task OnCompletedAsync(CancellationToken token = new CancellationToken());
            Task OnNextAsync(object value);
        }

        private class ObserverBridge<T> : IObserverBridge, IDisposableAsyncObservable<T>, IAsyncObserver<T>
        {
            private readonly List<IAsyncObserver<T>> observers;
            private readonly IAsyncDisposable asyncDisposable;
            private bool isDisposed;
            private bool isActive;

            public ObserverBridge(IAsyncDisposable asyncDisposable)
            {
                this.asyncDisposable = asyncDisposable;
                observers = new List<IAsyncObserver<T>>();
                isDisposed = false;
                isActive = true;
            }

            public async Task<IAsyncDisposable> SubscribeAsync(IAsyncObserver<T> observer, CancellationToken token = new CancellationToken())
            {
                if (isDisposed) throw new ObjectDisposedException("");
                if (!isActive)
                {
                    await observer.OnCompletedAsync(token);
                    return NoOpDisposable.Instance;
                }

                observers.Add(observer);
                return new DisposeAction(() => observers.Remove(observer));
            }

            public async Task DisposeAsync(CancellationToken token = new CancellationToken())
            {
                if (isDisposed) throw new ObjectDisposedException("");
                Task pending = asyncDisposable.DisposeAsync(token);
                isDisposed = true;
                await pending;
            }

            public Task OnNextAsync(T value, CancellationToken token = new CancellationToken())
            {
                if (isDisposed) throw new ObjectDisposedException("");
                List<Task> pending = observers.Select(obs => obs.OnNextAsync(value, token)).ToList();
                return Task.WhenAll(pending);
            }

            public Task OnErrorAsync(Exception error, CancellationToken token = new CancellationToken())
            {
                if (isDisposed) throw new ObjectDisposedException("");
                List<Task> pending = observers.Select(obs => obs.OnErrorAsync(error, token)).ToList();
                isActive = false;
                return Task.WhenAll(pending);
            }

            public Task OnCompletedAsync(CancellationToken token = new CancellationToken())
            {
                if (isDisposed) throw new ObjectDisposedException("");
                List<Task> pending = observers.Select(obs => obs.OnCompletedAsync(token)).ToList();
                isActive = false;
                return Task.WhenAll(pending);
            }

            public Task OnNextAsync(object value)
            {
                return OnNextAsync((T)value);
            }

            private class DisposeAction : IAsyncDisposable
            {
                private readonly Action disposeAction;
                private bool disposed;

                public DisposeAction(Action disposeAction)
                {
                    this.disposeAction = disposeAction;
                    disposed = false;
                }

                public Task DisposeAsync(CancellationToken token = new CancellationToken())
                {
                    if (disposed) throw new ObjectDisposedException("");
                    disposeAction();
                    disposed = false;
                    return TaskDone.Done;
                }
            }

            private class NoOpDisposable : IAsyncDisposable
            {
                public static IAsyncDisposable Instance = new NoOpDisposable();

                public Task DisposeAsync(CancellationToken token = new CancellationToken())
                {
                    return TaskDone.Done;
                }
            }
        }
    }
}
