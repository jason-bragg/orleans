using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Streams.Core;

namespace Orleans.Streams
{
    internal class StreamConsumer<T> : IInternalAsyncObservable<T>
    {
        internal bool                               IsRewindable { get; private set; }

        private readonly StreamImpl<T>              stream;
        private readonly string                     streamProviderName;
        [NonSerialized]
        private readonly IStreamProviderRuntime     providerRuntime;
        [NonSerialized]
        private readonly IStreamPubSub              pubSub;
        private StreamConsumerExtension             myExtension;
        private IStreamConsumerExtension            myGrainReference;
        [NonSerialized]
        private readonly AsyncLock                  bindExtLock;
        [NonSerialized]
        private readonly ILogger logger;

        public StreamConsumer(StreamImpl<T> stream, string streamProviderName, IStreamProviderRuntime providerUtilities, IStreamPubSub pubSub, ILoggerFactory loggerFactory, bool isRewindable)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (providerUtilities == null) throw new ArgumentNullException("providerUtilities");
            if (pubSub == null) throw new ArgumentNullException("pubSub");

            logger = loggerFactory.CreateLogger(string.Format("<{0}>-{1}", typeof(StreamConsumer<T>).FullName, stream));
            this.stream = stream;
            this.streamProviderName = streamProviderName;
            providerRuntime = providerUtilities;
            this.pubSub = pubSub;
            IsRewindable = isRewindable;
            myExtension = null;
            myGrainReference = null;
            bindExtLock = new AsyncLock();
        }

        public Task<IStreamSubscriptionHandle> SubscribeAsync(IAsyncObserver<T> observer)
        {
            return SubscribeAsync(observer, null);
        }

        public async Task<IStreamSubscriptionHandle> SubscribeAsync(
            IAsyncObserver<T> observer,
            StreamSequenceToken token,
            StreamFilterPredicate filterFunc = null,
            object filterData = null)
        {
            if (token != null && !IsRewindable)
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncObservable.");
            if (observer is GrainReference)
                throw new ArgumentException("On-behalf subscription via grain references is not supported. Only passing of object references is allowed.", "observer");

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Subscribe Observer={0} Token={1}", observer, token);
            await BindExtensionLazy();

            IStreamFilterPredicateWrapper filterWrapper = null;
            if (filterFunc != null)
                filterWrapper = new FilterPredicateWrapperData(filterData, filterFunc);
            
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Subscribe - Connecting to Rendezvous {0} My GrainRef={1} Token={2}",
                pubSub, myGrainReference, token);

            GuidId subscriptionId = pubSub.CreateSubscriptionId(stream.StreamId, myGrainReference);

            // Optimistic Concurrency: 
            // In general, we should first register the subsription with the pubsub (pubSub.RegisterConsumer)
            // and only if it succeeds store it locally (myExtension.SetObserver). 
            // Basicaly, those 2 operations should be done as one atomic transaction - either both or none and isolated from concurrent reads.
            // BUT: there is a distributed race here: the first msg may arrive before the call is awaited 
            // (since the pubsub notifies the producer that may immideately produce)
            // and will thus not find the subriptionHandle in the extension, basically violating "isolation". 
            // Therefore, we employ Optimistic Concurrency Control here to guarantee isolation: 
            // we optimisticaly store subscriptionId in the handle first before calling pubSub.RegisterConsumer
            // and undo it in the case of failure. 
            // There is no problem with that we call myExtension.SetObserver too early before the handle is registered in pub sub,
            // since this subscriptionId is unique (random Guid) and no one knows it anyway, unless successfully subscribed in the pubsub.
            var subriptionHandle = myExtension.CreateHandle(subscriptionId, this.stream.StreamId);
            myExtension.SetObserver(subriptionHandle, observer, token);
            try
            {
                await pubSub.RegisterConsumer(subscriptionId, stream.StreamId, streamProviderName, myGrainReference, filterWrapper);
                return subriptionHandle;
            } catch(Exception)
            {
                // Undo the previous call myExtension.SetObserver.
                myExtension.RemoveObserver(subscriptionId);
                throw;
            }            
        }

        public async Task<IStreamSubscriptionHandle> ResumeAsync(
            IStreamSubscriptionHandle handle,
            IAsyncObserver<T> observer,
            StreamSequenceToken token = null)
        {
            if (token != null && !IsRewindable)
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncObservable.");

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Resume Observer={0} Token={1}", observer, token);
            await BindExtensionLazy();

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Resume - Connecting to Rendezvous {0} My GrainRef={1} Token={2}",
                pubSub, myGrainReference, token);

            await handle.ResumeAsync<T>(observer, token);
            return handle;
        }

        public async Task UnsubscribeAsync(IStreamSubscriptionHandle handle)
        {
            await BindExtensionLazy();

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Unsubscribe StreamSubscriptionHandle={0}", handle);

            await handle.UnsubscribeAsync();
        }

        public async Task<IList<IStreamSubscriptionHandle>> GetAllSubscriptions()
        {
            await BindExtensionLazy();

            List<StreamSubscription> subscriptions= await pubSub.GetAllSubscriptions(stream.StreamId, myGrainReference);
            return subscriptions.Select(sub => this.myExtension.CreateHandle(GuidId.GetGuidId(sub.SubscriptionId), stream.StreamId))
                                  .ToList<IStreamSubscriptionHandle>();
        }

        public async Task Cleanup()
        {
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Cleanup() called");
            if (myExtension == null)
                return;

            var allHandles = myExtension.GetAllStreamHandles(this.stream.StreamId);
            var tasks = new List<Task>();
            foreach (var handle in allHandles)
            {
                myExtension.RemoveObserver(handle.SubscriptionId);
                tasks.Add(pubSub.UnregisterConsumer(handle.SubscriptionId, stream.StreamId, streamProviderName));
            }
            try
            {
                await Task.WhenAll(tasks);

            } catch (Exception exc)
            {
                logger.Warn(ErrorCode.StreamProvider_ConsumerFailedToUnregister,
                    "Ignoring unhandled exception during PubSub.UnregisterConsumer", exc);
            }
            myExtension = null;
        }

        // Used in test.
        internal bool InternalRemoveObserver(IStreamSubscriptionHandle handle)
        {
            return myExtension != null && myExtension.RemoveObserver(handle.SubscriptionId);
        }

        internal Task<int> DiagGetConsumerObserversCount()
        {
            return Task.FromResult(myExtension.DiagCountStreamObservers<T>(stream.StreamId));
        }

        private async Task BindExtensionLazy()
        {
            if (myExtension == null)
            {
                using (await bindExtLock.LockAsync())
                {
                    if (myExtension == null)
                    {
                        if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("BindExtensionLazy - Binding local extension to stream runtime={0}", providerRuntime);
                        var tup = await providerRuntime.BindExtension<StreamConsumerExtension, IStreamConsumerExtension>(
                            () => new StreamConsumerExtension(providerRuntime));
                        myExtension = tup.Item1;
                        myGrainReference = tup.Item2;
                        if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("BindExtensionLazy - Connected Extension={0} GrainRef={1}", myExtension, myGrainReference);                        
                    }
                }
            }
        }
    }
}
