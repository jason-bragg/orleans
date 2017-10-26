//#define USE_GENERICS

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using UnitTests.GrainInterfaces;
using UnitTests.StreamingTests;

namespace UnitTests.Grains
{
    public class StreamReliabilityTestGrainState
    {
        // For producer and consumer 
        // -- only need to store because of how we run our unit tests against multiple providers
        public string StreamProviderName { get; set; }

        // For producer only.
#if USE_GENERICS
        public IAsyncStream<T> Stream { get; set; }
#else
        public IAsyncStream<int> Stream { get; set; }
#endif

        public bool IsProducer { get; set; }
    }

    [Orleans.Providers.StorageProvider(ProviderName = "AzureStore")]
#if USE_GENERICS
    public class StreamReliabilityTestGrain<T> : Grain<IStreamReliabilityTestGrainState>, IStreamReliabilityTestGrain<T>
#else
    public class StreamReliabilityTestGrain : Grain<StreamReliabilityTestGrainState>, IStreamReliabilityTestGrain
#endif
    {
        [NonSerialized]
        private Logger logger;

#if USE_GENERICS
        private IAsyncStream<T> Stream { get; set; }
        private IAsyncObserver<T> Producer { get; set; }
        private Dictionary<IStreamSubscriptionHandle, MyStreamObserver<T>> Observers { get; set; }
#else
        private IAsyncStream<int> Stream { get { return State.Stream; } }
        private IAsyncObserver<int> Producer { get; set; }
        private Dictionary<IStreamSubscriptionHandle, MyStreamObserver<int>> Observers { get; set; }
#endif
        private const string StreamNamespace = StreamTestsConstants.StreamReliabilityNamespace;

        public override async Task OnActivateAsync()
        {
            logger = GetLogger("StreamReliabilityTestGrain-" + this.IdentityString);

            if (Observers == null)
#if USE_GENERICS
                Observers = new Dictionary<IStreamSubscriptionHandle, MyStreamObserver<T>>();
#else
                Observers = new Dictionary<IStreamSubscriptionHandle, MyStreamObserver<int>>();
#endif

            if (State.Stream != null && State.StreamProviderName != null)
            {
                IStreamProvider streamProvider = GetStreamProvider(this.State.StreamProviderName);
                IAsyncStream<int> stream = streamProvider.GetStream<int>(this.State.Stream.Guid, this.State.Stream.Namespace);
                IList<IStreamSubscriptionHandle> handles = await stream.GetAllSubscriptionHandles();
                logger.Info(String.Format("OnActivateAsync IsProducer = {0}, IsConsumer = {1}.",
                    State.IsProducer, handles != null && handles.Count > 0));
                if (handles.Count > 0)
                {
                    await ReconnectConsumerHandles(handles);
                }
                if (State.IsProducer)
                {
                    //await BecomeProducer(State.StreamId, State.StreamProviderName);
                    Producer = Stream;
                    State.IsProducer = true;
                    await WriteStateAsync();
                }
            }
            else
            {
                logger.Info("No stream yet.");
            }
        }

        public override Task OnDeactivateAsync()
        {
            logger.Info("OnDeactivateAsync");
            return base.OnDeactivateAsync();
        }

        public Task<int> GetConsumerCount()
        {
            int numConsumers = this.Observers.Count;
            logger.Info("ConsumerCount={0}", numConsumers);
            return Task.FromResult(numConsumers);
        }
        public Task<int> GetReceivedCount()
        {
            int numReceived = Observers.Sum(o => o.Value.NumItems);
            logger.Info("ReceivedCount={0}", numReceived);
            return Task.FromResult(numReceived);
        }
        public Task<int> GetErrorsCount()
        {
            int numErrors = Observers.Sum(o => o.Value.NumErrors);
            logger.Info("ErrorsCount={0}", numErrors);
            return Task.FromResult(numErrors);
        }

        public Task Ping()
        {
            logger.Info("Ping");
            return Task.CompletedTask;
        }

#if USE_GENERICS
        public async Task<IStreamSubscriptionHandle> AddConsumer(Guid streamId, string providerName)
#else
        public async Task<GuidId> AddConsumer(Guid streamId, string providerName)
#endif
        {
            logger.Info("AddConsumer StreamId={0} StreamProvider={1} Grain={2}", streamId, providerName, this.AsReference<IStreamReliabilityTestGrain>());
            TryInitStream(streamId, providerName);
#if USE_GENERICS
            var observer = new MyStreamObserver<T>();
#else
            var observer = new MyStreamObserver<int>(logger);
#endif
            var subsHandle = await Stream.SubscribeAsync(observer);
            Observers.Add(subsHandle, observer);
            await WriteStateAsync();
            return subsHandle.SubscriptionId;
        }

#if USE_GENERICS
        public async Task RemoveConsumer(Guid streamId, string providerName, IStreamSubscriptionHandle subsHandle)
#else
        public async Task RemoveConsumer(Guid streamId, string providerName, GuidId subscription)
#endif
        {
            logger.Info("RemoveConsumer StreamId={0} StreamProvider={1}", streamId, providerName);
            if (this.Observers.Count == 0) throw new InvalidOperationException("Not a Consumer");
            IStreamSubscriptionHandle subsHandle = this.Observers.Keys.FirstOrDefault(h => h.SubscriptionId == subscription);
            if (subsHandle == null) return;
            await subsHandle.UnsubscribeAsync();
            Observers.Remove(subsHandle);
            await WriteStateAsync();
        }

        public async Task RemoveAllConsumers()
        {
            logger.Info("RemoveAllConsumers: State.ConsumerSubscriptionHandles.Count={0}", this.Observers.Count);
            if (this.Observers.Count == 0) throw new InvalidOperationException("Not a Consumer");
            foreach (var handle in this.Observers.Keys)
            {
                await handle.UnsubscribeAsync();
            }
            //Observers.Remove(subsHandle);
            await WriteStateAsync();
        }

        public async Task BecomeProducer(Guid streamId, string providerName)
        {
            logger.Info("BecomeProducer StreamId={0} StreamProvider={1}", streamId, providerName);
            TryInitStream(streamId, providerName);
            Producer = Stream;
            State.IsProducer = true;
            await WriteStateAsync();
        }

        public async Task RemoveProducer(Guid streamId, string providerName)
        {
            logger.Info("RemoveProducer StreamId={0} StreamProvider={1}", streamId, providerName);
            if (!State.IsProducer) throw new InvalidOperationException("Not a Producer");
            Producer = null;
            State.IsProducer = false;
            await WriteStateAsync();
        }

        public async Task ClearGrain()
        {
            logger.Info("ClearGrain.");
            State.IsProducer = false;
            Observers.Clear();
            State.Stream = null;
            await ClearStateAsync();
        }

        public Task<bool> IsConsumer()
        {
            bool isConsumer = this.Observers.Count > 0;
            logger.Info("IsConsumer={0}", isConsumer);
            return Task.FromResult(isConsumer);
        }
        public Task<bool> IsProducer()
        {
            bool isProducer = State.IsProducer;
            logger.Info("IsProducer={0}", isProducer);
            return Task.FromResult(isProducer);
        }
        public Task<int> GetConsumerHandlesCount()
        {
            return Task.FromResult(this.Observers.Count);
        }

        public async Task<int> GetConsumerObserversCount()
        {
#if USE_GENERICS
            var consumer = (StreamConsumer<T>)Stream;
#else
            var consumer = (StreamConsumer<int>)Stream;
#endif
            return await consumer.DiagGetConsumerObserversCount();
        }


#if USE_GENERICS
        public async Task SendItem(T item)
#else
        public async Task SendItem(int item)
#endif
        {
            logger.Info("SendItem Item={0}", item);
            await Producer.OnNextAsync(item);
        }

        public Task<SiloAddress> GetLocation()
        {
            SiloAddress siloAddress = Data.Address.Silo;
            logger.Info("GetLocation SiloAddress={0}", siloAddress);
            return Task.FromResult(siloAddress);
        }

        private void TryInitStream(Guid streamId, string providerName)
        {
            if (providerName == null) throw new ArgumentNullException(nameof(providerName));

            State.StreamProviderName = providerName;

            if (State.Stream == null)
            {
                logger.Info("InitStream StreamId={0} StreamProvider={1}", streamId, providerName);

                IStreamProvider streamProvider = GetStreamProvider(providerName);
#if USE_GENERICS
                State.Stream = streamProvider.GetStream<T>(streamId);
#else
                State.Stream = streamProvider.GetStream<int>(streamId, StreamNamespace);
#endif
            }
        }

#if USE_GENERICS
        private async Task ReconnectConsumerHandles(IList<IStreamSubscriptionHandle> subscriptionHandles)
#else
        private async Task ReconnectConsumerHandles(IList<IStreamSubscriptionHandle> subscriptionHandles)
#endif
        {
            logger.Info("ReconnectConsumerHandles SubscriptionHandles={0} Grain={1}", Utils.EnumerableToString(subscriptionHandles), this.AsReference<IStreamReliabilityTestGrain>());


            foreach (var subHandle in subscriptionHandles)
            {
#if USE_GENERICS
                // var stream = GetStreamProvider(State.StreamProviderName).GetStream<T>(subHandle.StreamId);
                var stream = subHandle.Stream;
                var observer = new MyStreamObserver<T>();
#else
                var observer = new MyStreamObserver<int>(logger);
#endif
                var subsHandle = await subHandle.ResumeAsync(observer);
                Observers.Add(subsHandle, observer);
            }
            await WriteStateAsync();
        }
    }

    //[Serializable]
    //public class MyStreamObserver<T> : IAsyncObserver<T>
    //{
    //    internal int NumItems { get; private set; }
    //    internal int NumErrors { get; private set; }

    //    private readonly Logger logger;

    //    internal MyStreamObserver(Logger logger)
    //    {
    //        this.logger = logger;
    //    }

    //    public Task OnNextAsync(T item, StreamSequenceToken token)
    //    {
    //        NumItems++;
    //        if (logger.IsVerbose)
    //            logger.Verbose("Received OnNextAsync - Item={0} - Total Items={1} Errors={2}", item, NumItems, NumErrors);
    //        return Task.CompletedTask;
    //    }

    //    public Task OnCompletedAsync()
    //    {
    //        logger.Info("Receive OnCompletedAsync - Total Items={0} Errors={1}", NumItems, NumErrors);
    //        return Task.CompletedTask;
    //    }

    //    public Task OnErrorAsync(Exception ex)
    //    {
    //        NumErrors++;
    //        logger.Warn(1, "Received OnErrorAsync - Exception={0} - Total Items={1} Errors={2}", ex, NumItems, NumErrors);
    //        return Task.CompletedTask;
    //    }
    //}


    [Orleans.Providers.StorageProvider(ProviderName = "AzureStore")]
    public class StreamUnsubscribeTestGrain : Grain<StreamReliabilityTestGrainState>, IStreamUnsubscribeTestGrain
    {
        [NonSerialized]
        private Logger logger;

        private const string StreamNamespace = StreamTestsConstants.StreamReliabilityNamespace;

        public override async Task OnActivateAsync()
        {
            logger = GetLogger("StreamUnsubscribeTestGrain-" + this.IdentityString);
            if (State.Stream != null && State.StreamProviderName != null)
            {
                IStreamProvider streamProvider = GetStreamProvider(this.State.StreamProviderName);
                IAsyncStream<int> stream = streamProvider.GetStream<int>(this.State.Stream.Guid, this.State.Stream.Namespace);
                IList<IStreamSubscriptionHandle> handles = await stream.GetAllSubscriptionHandles();
                logger.Info(String.Format("OnActivateAsync IsProducer = {0}, IsConsumer = {1}.", State.IsProducer, handles != null && handles.Count > 0));
            }
        }

        public async Task Subscribe(Guid streamId, string providerName)
        {
            logger.Info("Subscribe StreamId={0} StreamProvider={1} Grain={2}", streamId, providerName, this.AsReference<IStreamUnsubscribeTestGrain>());

            State.StreamProviderName = providerName;
            if (State.Stream == null)
            {
                logger.Info("InitStream StreamId={0} StreamProvider={1}", streamId, providerName);
                IStreamProvider streamProvider = GetStreamProvider(providerName);
                State.Stream = streamProvider.GetStream<int>(streamId, StreamNamespace);
            }

            var observer = new MyStreamObserver<int>(logger);
            var consumer = State.Stream;
            var subsHandle = await consumer.SubscribeAsync(observer);
            await WriteStateAsync();
        }

        public async Task UnSubscribeFromAllStreams()
        {
            if (State.Stream != null && State.StreamProviderName != null)
            {
                IStreamProvider streamProvider = GetStreamProvider(this.State.StreamProviderName);
                IAsyncStream<int> stream = streamProvider.GetStream<int>(this.State.Stream.Guid, this.State.Stream.Namespace);
                IList<IStreamSubscriptionHandle> handles = await stream.GetAllSubscriptionHandles();
                if (handles.Count == 0) throw new InvalidOperationException("Not a Consumer");
                logger.Info("UnSubscribeFromAllStreams: Subscription Count={0}", handles.Count);
                foreach (var handle in handles)
                {
                    await handle.UnsubscribeAsync();
                }
            }
        }
    }
}