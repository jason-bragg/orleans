using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using Microsoft.Extensions.Logging;

namespace Orleans.Providers.Streams.SimpleMessageStream
{
    internal class SimpleMessageStreamProducer<T> : IInternalAsyncBatchObserver<T>
    {
        private readonly StreamImpl<T>                  stream;
        private readonly string                         streamProviderName;

        [NonSerialized]
        private readonly SerializationManager serializationManager;

        [NonSerialized]
        private readonly IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar;
        [NonSerialized]
        private readonly IStreamSubscriptionManifest<Guid, IStreamIdentity> subscriptionManifest;

        [NonSerialized]
        private readonly IStreamProviderRuntime         providerRuntime;
        private SimpleMessageStreamProducerExtension    myExtension;
        private IStreamProducerExtension                myGrainReference;
        private IAsyncLinkedListNode<IList<StreamSubscription<Guid>>> subscriptions;
        private readonly bool                           fireAndForgetDelivery;
        private readonly bool                           optimizeForImmutableData;
        [NonSerialized]
        private bool                                    isDisposed;
        [NonSerialized]
        private readonly ILogger                         logger;
        [NonSerialized]
        private readonly AsyncLock                      initLock;
        [NonSerialized]
        private readonly ILoggerFactory loggerFactory;
        internal bool IsRewindable { get; private set; }

        internal SimpleMessageStreamProducer(
            StreamImpl<T> stream,
            string streamProviderName,
            IStreamProviderRuntime providerUtilities,
            bool fireAndForgetDelivery,
            bool optimizeForImmutableData,
            IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar,
            IStreamSubscriptionManifest<Guid, IStreamIdentity> subscriptionManifest,
            bool isRewindable,
            SerializationManager serializationManager,
            ILoggerFactory loggerFactory)
        {
            this.stream = stream;
            this.streamProviderName = streamProviderName;
            providerRuntime = providerUtilities;
            this.subscriptionRegistrar = subscriptionRegistrar;
            this.subscriptionManifest = subscriptionManifest;
            this.serializationManager = serializationManager;
            this.fireAndForgetDelivery = fireAndForgetDelivery;
            this.optimizeForImmutableData = optimizeForImmutableData;
            IsRewindable = isRewindable;
            isDisposed = false;
            initLock = new AsyncLock();
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory.CreateLogger(this.GetType().FullName);
            ConnectToRendezvous().Ignore();
        }

        private async Task BindExtensions()
        {
            var tup = await providerRuntime.BindExtension<SimpleMessageStreamProducerExtension, IStreamProducerExtension>(
                () => new SimpleMessageStreamProducerExtension(providerRuntime, this.subscriptionRegistrar, this.loggerFactory, fireAndForgetDelivery, optimizeForImmutableData));

            myExtension = tup.Item1;
            myGrainReference = tup.Item2;

            myExtension.AddStream(stream.StreamId);
        }

        private async Task ConnectToRendezvous()
        {
            if (isDisposed)
                throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "ConnectToRendezvous"));

            // the caller should check _connectedToRendezvous before calling this method.
            using (await initLock.LockAsync())
            {
                if (this.subscriptions == null) // need to re-check again.
                {
                    await BindExtensions();
                    this.subscriptions = await this.subscriptionManifest.MonitorSubscriptions(stream.StreamId);
                    myExtension.AddSubscribers(stream.StreamId, this.subscriptions.Value);
                }
            }
        }

        public async Task OnNextAsync(T item, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            

            if (isDisposed) throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "OnNextAsync"));

            if (this.subscriptions == null)
            {
                if (!this.optimizeForImmutableData)
                {
                    // In order to avoid potential concurrency errors, synchronously copy the input before yielding the
                    // thread. DeliverItem below must also be take care to avoid yielding before copying for non-immutable objects.
                    item = (T) this.serializationManager.DeepCopy(item);
                }

                await ConnectToRendezvous();
            }

            await myExtension.DeliverItem(stream.StreamId, item);
        }

        public Task OnNextBatchAsync(IEnumerable<T> batch, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable) throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            
            throw new NotImplementedException("We still don't support OnNextBatchAsync()");
        }

        public async Task OnCompletedAsync()
        {
            if (isDisposed) throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "OnCompletedAsync"));

            if (this.subscriptions == null)
                await ConnectToRendezvous();

            await myExtension.CompleteStream(stream.StreamId);
        }

        public async Task OnErrorAsync(Exception exc)
        {
            if (isDisposed) throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "OnErrorAsync"));

            if (this.subscriptions == null)
                await ConnectToRendezvous();

            await myExtension.ErrorInStream(stream.StreamId, exc);
        }

        internal Action OnDisposeTestHook { get; set; }

        public Task Cleanup()
        {
            if(logger.IsEnabled(LogLevel.Debug)) logger.Debug("Cleanup() called");

            myExtension.RemoveStream(stream.StreamId);

            if (isDisposed)
            {
                return Task.CompletedTask;
            }

            if (this.subscriptions != null)
            {
                try
                {
                    this.subscriptions.Dispose();
                    this.subscriptions = null;
                }
                catch (Exception exc)
                {
                    logger.Warn((int) ErrorCode.StreamProvider_ProducerFailedToUnregister,
                        "Ignoring unhandled exception during PubSub.UnregisterProducer", exc);
                }
            }
            isDisposed = true;

            Action onDisposeTestHook = OnDisposeTestHook; // capture
            if (onDisposeTestHook != null)
                onDisposeTestHook();
            return Task.CompletedTask;
        }
    }
}
