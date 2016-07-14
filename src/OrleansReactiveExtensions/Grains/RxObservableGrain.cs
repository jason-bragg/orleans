
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public class RxObservableGrain : Grain, IRxObservableGrain
    {
        private Dictionary<string, Topic> topics;
        
        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            topics = new Dictionary<string, Topic>();
        }

        public override async Task OnDeactivateAsync()
        {
            await base.OnDeactivateAsync();
            List<Task> pending = topics.Values.Select(bridge => bridge.OnCompletedAsync()).ToList();
            await Task.WhenAll(pending);
        }

        public IAsyncObserver<T> Publish<T>(string topicName)
        {
            Topic topic;
            if (!topics.TryGetValue(topicName, out topic))
            {
                topic = new Topic<T>(topicName);
                topics.Add(topicName, topic);
            }
            return (IAsyncObserver<T>)topic;
        }
        
        public Task<IAsyncDisposable> SubscribeAsync(string topicName, Guid subscriptionId, IRxObserverGrain observerGrain)
        {
            Topic topic;
            if (!topics.TryGetValue(topicName, out topic))
            {
                throw new ArgumentOutOfRangeException($"Topic {topicName} not found");
            }

            topic.Add(subscriptionId, observerGrain);

            return Task.FromResult<IAsyncDisposable>(new AsyncDisposable(topicName, subscriptionId, this.AsReference<IRxObservableGrain>()));
        }

        public Task UnsubscribeAsync(string topicName, Guid subscriptionId)
        {
            Topic topic;
            if (!topics.TryGetValue(topicName, out topic))
            {
                throw new ArgumentOutOfRangeException($"Topic {topicName} not found");
            }
            topic.Remove(subscriptionId);
            return TaskDone.Done;
        }

        public IAsyncObserver<object> OpenTopic(string topicName)
        {
            var topic = new Topic(topicName);
            topics.Add(topicName, topic);
            return topic;
        }

        [Serializable]
        private class AsyncDisposable : IAsyncDisposable
        {
            private readonly string topic;
            private readonly Guid subscriptionId;
            private readonly IRxObservableGrain observableGrain;

            public AsyncDisposable(string topic, Guid subscriptionId, IRxObservableGrain observableGrain)
            {
                this.topic = topic;
                this.subscriptionId = subscriptionId;
                this.observableGrain = observableGrain;
            }

            public Task DisposeAsync(CancellationToken token = new CancellationToken())
            {
                return observableGrain.UnsubscribeAsync(topic, subscriptionId);
            }
        }

        private class Topic : IAsyncObserver<object>
        {
            protected readonly Dictionary<Guid, IRxObserverGrain> observers;
            protected readonly string topic;
            protected bool IsActive { get; private set; }

            public Topic(string topic)
            {
                this.topic = topic;
                observers = new Dictionary<Guid, IRxObserverGrain>();
                IsActive = true;
            }

            public void Add(Guid subscriptionId, IRxObserverGrain observerGrain)
            {
                observers.Add(subscriptionId, observerGrain);
                if (!IsActive)
                {
                    observerGrain.OnCompletedAsync(topic, subscriptionId).Ignore();
                }
            }

            public void Remove(Guid subscriptionId)
            {
                observers.Remove(subscriptionId);
            }

            public Task DisposeAsync(CancellationToken token = new CancellationToken())
            {
                throw new InvalidOperationException("Observable can not dispose of observers.");
            }

            public Task OnNextAsync(object value, CancellationToken token = new CancellationToken())
            {
                if (!IsActive) return TaskDone.Done;
                List<Task> notifyErrors = observers.Select(kvp => kvp.Value.OnNextAsync(topic, kvp.Key, value)).ToList();
                return Task.WhenAll(notifyErrors);
            }

            public Task OnErrorAsync(Exception error, CancellationToken token = new CancellationToken())
            {
                if (!IsActive) return TaskDone.Done;
                List<Task> notifyErrors = observers.Select(kvp => kvp.Value.OnErrorAsync(topic, kvp.Key, error)).ToList();
                IsActive = false;
                return Task.WhenAll(notifyErrors);
            }

            public Task OnCompletedAsync(CancellationToken token = new CancellationToken())
            {
                if (!IsActive) return TaskDone.Done;
                List<Task> notifyErrors = observers.Select(kvp => kvp.Value.OnCompletedAsync(topic, kvp.Key)).ToList();
                IsActive = false;
                return Task.WhenAll(notifyErrors);
            }
        }

        private class Topic<T> : Topic, IAsyncObserver<T>
        {
            public Topic(string topic) : base(topic) { }

            public Task OnNextAsync(T value, CancellationToken token = new CancellationToken())
            {
                if (!IsActive) return TaskDone.Done;
                List<Task> notifyErrors = observers.Select(kvp => kvp.Value.OnNextAsync(topic, kvp.Key, value)).ToList();
                return Task.WhenAll(notifyErrors);
            }
        }
    }
}
