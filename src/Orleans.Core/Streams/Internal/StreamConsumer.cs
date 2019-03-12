using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

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
        private readonly IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar;
        private StreamConsumerExtension             myExtension;
        private IStreamConsumerExtension            myGrainReference;
        [NonSerialized]
        private readonly AsyncLock                  bindExtLock;
        [NonSerialized]
        private readonly ILogger logger;

        public StreamConsumer(
            StreamImpl<T> stream,
            string streamProviderName,
            IStreamProviderRuntime runtime,
            IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar,
            ILogger logger,
            bool isRewindable)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.stream = stream ?? throw new ArgumentNullException(nameof(stream));
            this.streamProviderName = streamProviderName;
            this.providerRuntime = runtime ?? throw new ArgumentNullException(nameof(runtime));
            this.registrar = registrar ?? throw new ArgumentNullException(nameof(registrar));
            this.IsRewindable = isRewindable;
            this.myExtension = null;
            this.myGrainReference = null;
            this.bindExtLock = new AsyncLock();
        }

        public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncObserver<T> observer)
        {
            return SubscribeAsyncImpl(observer, null, null);
        }

        public Task<StreamSubscriptionHandle<T>> SubscribeAsync(
            IAsyncObserver<T> observer,
            StreamSequenceToken token)
        {
            return SubscribeAsyncImpl(observer, null, token);
        }

        public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncBatchObserver<T> batchObserver)
        {
            return SubscribeAsyncImpl(null, batchObserver, null);
        }

        public Task<StreamSubscriptionHandle<T>> SubscribeAsync(IAsyncBatchObserver<T> batchObserver, StreamSequenceToken token)
        {
            return SubscribeAsyncImpl(null, batchObserver, token);
        }

        private async Task<StreamSubscriptionHandle<T>> SubscribeAsyncImpl(
            IAsyncObserver<T> observer,
            IAsyncBatchObserver<T> batchObserver,
            StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncObservable.");
            if (observer is GrainReference)
                throw new ArgumentException("On-behalf subscription via grain references is not supported. Only passing of object references is allowed.", nameof(observer));
            if (batchObserver is GrainReference)
                throw new ArgumentException("On-behalf subscription via grain references is not supported. Only passing of object references is allowed.", nameof(batchObserver));

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Subscribe Token={Token}", token);

            var consumer = myGrainReference as GrainReference;
            Guid subscription = this.registrar.CreateSubscription(stream.StreamId, consumer);
            GuidId subscriptionId = GuidId.GetGuidId(subscription);

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
            var subriptionHandle = myExtension.SetObserver(subscriptionId, stream, observer, batchObserver, token);
            try
            {
                await registrar.Register(stream.StreamId, subscription, consumer);
                return subriptionHandle;
            }
            catch (Exception)
            {
                // Undo the previous call myExtension.SetObserver.
                myExtension.RemoveObserver(subscriptionId);
                throw;
            }
        }

        public Task<StreamSubscriptionHandle<T>> ResumeAsync(
            StreamSubscriptionHandle<T> handle,
            IAsyncObserver<T> observer,
            StreamSequenceToken token = null)
        {
            return ResumeAsyncImpl(handle, observer, null, token);
        }

        public Task<StreamSubscriptionHandle<T>> ResumeAsync(
            StreamSubscriptionHandle<T> handle,
            IAsyncBatchObserver<T> batchObserver,
            StreamSequenceToken token = null)
        {
            return ResumeAsyncImpl(handle, null, batchObserver, token);
        }

        private async Task<StreamSubscriptionHandle<T>> ResumeAsyncImpl(
            StreamSubscriptionHandle<T> handle,
            IAsyncObserver<T> observer,
            IAsyncBatchObserver<T> batchObserver,
            StreamSequenceToken token = null)
        {
            StreamSubscriptionHandleImpl<T> oldHandleImpl = CheckHandleValidity(handle);

            if (token != null && !IsRewindable)
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncObservable.");

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Resume Token={Token}", token);
            await BindExtensionLazy();

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Resume - Connecting to Rendezvous. My GrainRef={0} Token={1}",
                myGrainReference, token);

            StreamSubscriptionHandle<T> newHandle = myExtension.SetObserver(oldHandleImpl.SubscriptionId, stream, observer, batchObserver, token);

            // On failure caller should be able to retry using the original handle, so invalidate old handle only if everything succeeded.  
            oldHandleImpl.Invalidate();

            return newHandle;
        }

        public async Task UnsubscribeAsync(StreamSubscriptionHandle<T> handle)
        {
            await BindExtensionLazy();

            StreamSubscriptionHandleImpl<T> handleImpl = CheckHandleValidity(handle);

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Unsubscribe StreamSubscriptionHandle={0}", handle);

            myExtension.RemoveObserver(handleImpl.SubscriptionId);
            // UnregisterConsumer from pubsub even if does not have this handle localy, to allow UnsubscribeAsync retries.

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Unsubscribe - Disconnecting from Rendezvous.  My GrainRef={0}",
                myGrainReference);

            await this.registrar.Unregister(stream.StreamId, handleImpl.SubscriptionId.Guid);

            handleImpl.Invalidate();
        }

        public async Task<IList<StreamSubscriptionHandle<T>>> GetAllSubscriptions()
        {
            await BindExtensionLazy();

            IList<StreamSubscription<Guid>> subscriptions = await this.registrar.GetConsumerSubscriptions(stream.StreamId, (GrainReference)this.myGrainReference);
            return subscriptions
                .Select(sub => new StreamSubscriptionHandleImpl<T>(GuidId.GetGuidId(sub.SubscriptionId), stream))
                .ToList<StreamSubscriptionHandle<T>>();
        }

        public async Task Cleanup()
        {
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Cleanup() called");
            if (myExtension == null)
                return;

            var allHandles = myExtension.GetAllStreamHandles<T>();
            var tasks = new List<Task>();
            foreach (var handle in allHandles)
            {
                myExtension.RemoveObserver(handle.SubscriptionId);
                tasks.Add(this.registrar.Unregister(stream.StreamId, handle.SubscriptionId.Guid));
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
        internal bool InternalRemoveObserver(StreamSubscriptionHandle<T> handle)
        {
            return myExtension != null && myExtension.RemoveObserver(((StreamSubscriptionHandleImpl<T>)handle).SubscriptionId);
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

        private StreamSubscriptionHandleImpl<T> CheckHandleValidity(StreamSubscriptionHandle<T> handle)
        {
            if (handle == null)
                throw new ArgumentNullException("handle");
            if (!handle.StreamIdentity.Equals(stream))
                throw new ArgumentException("Handle is not for this stream.", "handle");
            var handleImpl = handle as StreamSubscriptionHandleImpl<T>;
            if (handleImpl == null)
                throw new ArgumentException("Handle type not supported.", "handle");
            if (!handleImpl.IsValid)
                throw new ArgumentException("Handle is no longer valid.  It has been used to unsubscribe or resume.", "handle");
            return handleImpl;
        }
    }
}
