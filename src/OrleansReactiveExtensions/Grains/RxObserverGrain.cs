
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Rx.AsyncRx;

namespace Orleans.Rx
{
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
            List<Task> pending = bridges.Values.Select(bridge => bridge.Disposer.DisposeAsync()).ToList();
            await Task.WhenAll(pending);
            await base.OnDeactivateAsync();
        }

        public IAsyncConnectableObservable<T> Subscribe<T>(IRxObservableGrain observableGrain, string topic)
        {
            Guid subscriptionId = Guid.NewGuid();
            var bridge = new ObserverBridge<T>(topic, subscriptionId, observableGrain, this.AsReference<IRxObserverGrain>());
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

        private interface IObserverBridge
        {
            Task OnErrorAsync(Exception error, CancellationToken token = new CancellationToken());
            Task OnCompletedAsync(CancellationToken token = new CancellationToken());
            Task OnNextAsync(object value);
            IAsyncDisposable Disposer { get; }
        }

        private class ObserverBridge<T> : IObserverBridge, IAsyncConnectableObservable<T>, IAsyncObserver<T>
        {
            private readonly string topic;
            private readonly Guid subscriptionId;
            private readonly IRxObservableGrain observableGrain;
            private readonly IRxObserverGrain observerGrain;
            private readonly List<IAsyncObserver<T>> observers;
            private bool isActive;

            public IAsyncDisposable Disposer { get; private set; }

            public ObserverBridge(string topic, Guid subscriptionId, IRxObservableGrain observableGrain, IRxObserverGrain observerGrain)
            {
                this.topic = topic;
                this.subscriptionId = subscriptionId;
                this.observableGrain = observableGrain;
                this.observerGrain = observerGrain;
                observers = new List<IAsyncObserver<T>>();
            }

            public Task<IAsyncDisposable> SubscribeAsync(IAsyncObserver<T> observer, CancellationToken token = new CancellationToken())
            {
                observers.Add(observer);
                return Task.FromResult<IAsyncDisposable>(new DisposeAction(() => observers.Remove(observer)));
            }

            public async Task<IAsyncDisposable> Connect()
            {
                Disposer = await observableGrain.SubscribeAsync(topic, subscriptionId, observerGrain);
                isActive = true;
                return Disposer;
            }

            public Task OnNextAsync(T value, CancellationToken token = new CancellationToken())
            {
                if (!isActive) return TaskDone.Done;
                List<Task> pending = observers.Select(obs => obs.OnNextAsync(value, token)).ToList();
                return Task.WhenAll(pending);
            }

            public Task OnErrorAsync(Exception error, CancellationToken token = new CancellationToken())
            {
                if (!isActive) return TaskDone.Done;
                List<Task> pending = observers.Select(obs => obs.OnErrorAsync(error, token)).ToList();
                isActive = false;
                return Task.WhenAll(pending);
            }

            public Task OnCompletedAsync(CancellationToken token = new CancellationToken())
            {
                if (!isActive) return TaskDone.Done;
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

            public Task DisposeAsync(CancellationToken token = new CancellationToken())
            {
                throw new NotImplementedException();
            }
        }
    }
}
