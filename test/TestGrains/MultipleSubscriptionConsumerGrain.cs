using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    public class MultipleSubscriptionConsumerGrain : Grain, IMultipleSubscriptionConsumerGrain
    {
        private readonly Dictionary<IStreamSubscriptionHandle, Tuple<Counter,Counter>> consumedMessageCounts;
        private Logger logger;
        private int consumerCount = 0;

        private class Counter
        {
            public int Value { get; private set; }

            public void Increment()
            {
                Value++;
            }

            public void Clear()
            {
                Value = 0;
            }
        }

        public MultipleSubscriptionConsumerGrain()
        {
            consumedMessageCounts = new Dictionary<IStreamSubscriptionHandle, Tuple<Counter, Counter>>();
        }

        public override Task OnActivateAsync()
        {
            logger = base.GetLogger("MultipleSubscriptionConsumerGrain " + base.IdentityString);
            logger.Info("OnActivateAsync");
            return Task.CompletedTask;
        }

        public async Task<GuidId> BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse)
        {
            logger.Info("BecomeConsumer");

            // new counter for this subscription
            var count = new Counter();
            var error = new Counter();

            // get stream
            IStreamProvider streamProvider = GetStreamProvider(providerToUse);
            var stream = streamProvider.GetStream<int>(streamId, streamNamespace);

            int countCapture = consumerCount;
            consumerCount++;
            // subscribe
            IStreamSubscriptionHandle handle = await stream.SubscribeAsync(
                (e, t) => OnNext(e, t, countCapture, count),
                e => OnError(e, countCapture, error));

            // track counter
            consumedMessageCounts.Add(handle, Tuple.Create(count,error));

            // return handle
            return handle.SubscriptionId;
        }

        public async Task Resume(GuidId subscription, Guid streamId, string streamNamespace, string providerToUse)
        {
            logger.Info("Resume");

            // new counter for this subscription
            IStreamSubscriptionHandle handle = consumedMessageCounts.Keys.FirstOrDefault(h => h.SubscriptionId == subscription);
            Tuple<Counter, Counter> counters = null;
            if (handle != null)
            {
                consumedMessageCounts.TryGetValue(handle, out counters);
                consumedMessageCounts.Remove(handle);
            } else
            {
                IStreamProvider streamProvider = GetStreamProvider(providerToUse);
                var stream = streamProvider.GetStream<int>(streamId, streamNamespace);
                handle = (await stream.GetAllSubscriptionHandles()).FirstOrDefault(h => h.SubscriptionId == subscription);
            }
            counters = counters ?? Tuple.Create(new Counter(), new Counter());

            int countCapture = consumerCount++;
            // subscribe
            IStreamSubscriptionHandle newhandle = await handle.ResumeAsync<int>(
                (e, t) => OnNext(e, t, countCapture, counters.Item1),
                e => OnError(e, countCapture, counters.Item2));

            // track counter
            consumedMessageCounts[newhandle] = counters;
        }

        public async Task StopConsuming(GuidId subscription)
        {
            IStreamSubscriptionHandle handle = consumedMessageCounts.Keys.FirstOrDefault(h => h.SubscriptionId == subscription);
            if (handle == null)
                return;

            // unsubscribe
            await handle.UnsubscribeAsync();

            // stop tracking event count for stream
            consumedMessageCounts.Remove(handle);
        }

        public async Task<IList<GuidId>> GetAllSubscriptions(Guid streamId, string streamNamespace, string providerToUse)
        {
            logger.Info("GetAllSubscriptionHandles");

            // get stream
            IStreamProvider streamProvider = GetStreamProvider(providerToUse);
            var stream = streamProvider.GetStream<int>(streamId, streamNamespace);

            // get all active subscription handles for this stream.
            IList<IStreamSubscriptionHandle> handles = await stream.GetAllSubscriptionHandles();
            return handles.Select(h => h.SubscriptionId).ToList();
        }

        public Task<Dictionary<GuidId, Tuple<int,int>>> GetNumberConsumed()
        {
            logger.Info(String.Format("ConsumedMessageCounts = \n{0}", 
                Utils.EnumerableToString(consumedMessageCounts, kvp => String.Format("Consumer: {0} -> count: {1}", kvp.Key.SubscriptionId.ToString(), kvp.Value.ToString()))));

            return Task.FromResult(consumedMessageCounts.ToDictionary(kvp => kvp.Key.SubscriptionId, kvp => Tuple.Create(kvp.Value.Item1.Value, kvp.Value.Item2.Value)));
        }

        public Task ClearNumberConsumed()
        {
            logger.Info("ClearNumberConsumed");
            foreach (var counters in consumedMessageCounts.Values)
            {
                counters.Item1.Clear();
                counters.Item2.Clear();
            }
            return Task.CompletedTask;
        }

        public Task Deactivate()
        {
            DeactivateOnIdle();
            return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync()
        {
            logger.Info("OnDeactivateAsync");
            return Task.CompletedTask;
        }

        private Task OnNext(int e, StreamSequenceToken token, int countCapture, Counter count)
        {
            logger.Info("Got next event {0} on handle {1}", e, countCapture);
            var contextValue = RequestContext.Get(SampleStreaming_ProducerGrain.RequestContextKey) as string;
            if (!String.Equals(contextValue, SampleStreaming_ProducerGrain.RequestContextValue))
            {
                throw new Exception(String.Format("Got the wrong RequestContext value {0}.", contextValue));
            }
            count.Increment();
            return Task.CompletedTask;
        }

        private Task OnError(Exception e, int countCapture, Counter error)
        {
            logger.Info("Got exception {0} on handle {1}", e.ToString(), countCapture);
            error.Increment();
            return Task.CompletedTask;
        }
    }
}
