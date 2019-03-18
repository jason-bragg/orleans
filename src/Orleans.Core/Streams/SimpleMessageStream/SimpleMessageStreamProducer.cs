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
        private readonly SimpleMessageStreamSubscriptionManager subscriptionManager;
        private ChangeFeed<IList<StreamSubscription<Guid>>> subscriptionChangeFeed;
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
            this.subscriptionManager = new SimpleMessageStreamSubscriptionManager(stream.StreamId, providerUtilities, subscriptionRegistrar, loggerFactory, fireAndForgetDelivery, optimizeForImmutableData);
            ConnectToRendezvous().Ignore();
        }

        private async Task ConnectToRendezvous()
        {
            if (isDisposed)
                throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "ConnectToRendezvous"));

            // the caller should check _connectedToRendezvous before calling this method.
            using (await initLock.LockAsync())
            {
                if (this.subscriptionChangeFeed == null) // need to re-check again.
                {
                    this.subscriptionChangeFeed = await this.subscriptionManifest.GetSubscriptionChanges(stream.StreamId);
                    this.subscriptionManager.UpdateSubscribers(this.subscriptionChangeFeed.Value);
                }
            }
        }

        public async Task OnNextAsync(T item, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable)
                throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            

            if (isDisposed) throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "OnNextAsync"));

            if (this.subscriptionChangeFeed == null)
            {
                if (!this.optimizeForImmutableData)
                {
                    // In order to avoid potential concurrency errors, synchronously copy the input before yielding the
                    // thread. DeliverItem below must also be take care to avoid yielding before copying for non-immutable objects.
                    item = (T) this.serializationManager.DeepCopy(item);
                }

                await ConnectToRendezvous();
            } else
            {
                UpdateSubscriptions();
            }

            await this.subscriptionManager.DeliverItem(item);
        }

        public Task OnNextBatchAsync(IEnumerable<T> batch, StreamSequenceToken token)
        {
            if (token != null && !IsRewindable) throw new ArgumentNullException("token", "Passing a non-null token to a non-rewindable IAsyncBatchObserver.");
            
            throw new NotImplementedException("We still don't support OnNextBatchAsync()");
        }

        public async Task OnCompletedAsync()
        {
            if (isDisposed) throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "OnCompletedAsync"));

            if (this.subscriptionChangeFeed == null)
                await ConnectToRendezvous();

            await this.subscriptionManager.CompleteStream();
        }

        public async Task OnErrorAsync(Exception exc)
        {
            if (isDisposed) throw new ObjectDisposedException(string.Format("{0}-{1}", GetType(), "OnErrorAsync"));

            if (this.subscriptionChangeFeed == null)
                await ConnectToRendezvous();

            await this.subscriptionManager.ErrorInStream(exc);
        }

        internal Action OnDisposeTestHook { get; set; }

        public Task Cleanup()
        {
            if(logger.IsEnabled(LogLevel.Debug)) logger.Debug("Cleanup() called");

            if (isDisposed)
            {
                return Task.CompletedTask;
            }

            if (this.subscriptionChangeFeed != null)
            {
                try
                {
                    this.subscriptionChangeFeed.Dispose();
                    this.subscriptionChangeFeed = null;
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

        private void UpdateSubscriptions()
        {
            if (!this.subscriptionChangeFeed.Next.IsCompleted)
                return;

            // find most recent subscription list and only process that.
            while (this.subscriptionChangeFeed.Next.IsCompleted)
            {
                // should be non-blocking call, as we've already checked to see that the task is complete
                this.subscriptionChangeFeed = this.subscriptionChangeFeed.Next.GetResult();
            }

            this.subscriptionManager.UpdateSubscribers(this.subscriptionChangeFeed.Value);
        }
    }
}
