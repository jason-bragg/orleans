using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Streams.Core;

namespace Orleans.Streams
{
    internal interface IStreamObserver
    {
        IStreamSubscriptionHandle Handle { get; }
        Task<StreamHandshakeToken> DeliverItem(object item, StreamSequenceToken currentToken, StreamHandshakeToken handshakeToken);
        Task<StreamHandshakeToken> DeliverBatch(IBatchContainer item, StreamHandshakeToken handshakeToken);
        Task ErrorInStream(Exception exc);
        StreamHandshakeToken GetSequenceToken();
    }

    /// <summary>
    /// The extesion multiplexes all stream related messages to this grain between different streams and their stream observers.
    /// 
    /// On the silo, we have one extension object per activation and this extesion multiplexes all streams on this activation 
    ///     (streams of all types and ids: different stream ids and different stream providers).
    /// On the client, we have one extension per stream (we bind an extesion for every StreamConsumer, therefore every stream has its own extension).
    /// </summary>
    internal class StreamConsumerExtension : IStreamConsumerExtension
    {
        private readonly IStreamProviderRuntime providerRuntime;
        private readonly IStreamSubscriptionObserver streamSubscriptionObserver;
        private readonly ConcurrentDictionary<GuidId, IStreamObserver> allStreamObservers;
        private readonly ConcurrentDictionary<string, IInternalStreamProvider> rewindablityCache;
        private readonly Logger logger;
        private const int MAXIMUM_ITEM_STRING_LOG_LENGTH = 128;
        // if this extension is attached to a cosnumer grain which implements IOnSubscriptionActioner,
        // then this will be not null, otherwise, it will be null

        internal StreamConsumerExtension(IStreamProviderRuntime providerRt, IStreamSubscriptionObserver streamSubscriptionObserver = null)
        {
            this.providerRuntime = providerRt ?? throw new ArgumentNullException(nameof(providerRt));
            this.streamSubscriptionObserver = streamSubscriptionObserver;
            this.allStreamObservers = new ConcurrentDictionary<GuidId, IStreamObserver>();
            this.rewindablityCache = new ConcurrentDictionary<string, IInternalStreamProvider>();
            this.logger = providerRuntime.GetLogger(GetType().Name);
        }

        public Task<StreamHandshakeToken> DeliverImmutable(GuidId subscriptionId, StreamId streamId, Immutable<object> item, StreamSequenceToken currentToken, StreamHandshakeToken handshakeToken)
        {
            return DeliverMutable(subscriptionId, streamId, item.Value, currentToken, handshakeToken);
        }

        public async Task<StreamHandshakeToken> DeliverMutable(GuidId subscriptionId, StreamId streamId, object item, StreamSequenceToken currentToken, StreamHandshakeToken handshakeToken)
        {
            if (logger.IsVerbose3)
            {
                var itemString = item.ToString();
                itemString = (itemString.Length > MAXIMUM_ITEM_STRING_LOG_LENGTH) ? itemString.Substring(0, MAXIMUM_ITEM_STRING_LOG_LENGTH) + "..." : itemString;
                logger.Verbose3("DeliverItem {0} for subscription {1}", itemString, subscriptionId);
            }
            if (allStreamObservers.TryGetValue(subscriptionId, out IStreamObserver observer))
            {
                return await observer.DeliverItem(item, currentToken, handshakeToken);
            }
            else if(this.streamSubscriptionObserver != null)
            {
                IStreamSubscriptionHandle handle = CreateHandle(subscriptionId, streamId);
                await this.streamSubscriptionObserver.OnSubscribed(handle);
                //check if an observer were attached after handling the new subscription, deliver on it if attached
                if (allStreamObservers.TryGetValue(subscriptionId, out observer))
                {
                    return await observer.DeliverItem(item, currentToken, handshakeToken);
                }
            }

            logger.Warn((int)(ErrorCode.StreamProvider_NoStreamForItem), "{0} got an item for subscription {1}, but I don't have any subscriber for that stream. Dropping on the floor.",
                providerRuntime.ExecutingEntityIdentity(), subscriptionId);
            // We got an item when we don't think we're the subscriber. This is a normal race condition.
            // We can drop the item on the floor, or pass it to the rendezvous, or ...
            return default(StreamHandshakeToken);
        }

        public async Task<StreamHandshakeToken> DeliverBatch(GuidId subscriptionId, StreamId streamId, Immutable<IBatchContainer> batch, StreamHandshakeToken handshakeToken)
        {
            if (logger.IsVerbose3) logger.Verbose3("DeliverBatch {0} for subscription {1}", batch.Value, subscriptionId);

            if (allStreamObservers.TryGetValue(subscriptionId, out IStreamObserver observer))
            {
                return await observer.DeliverBatch(batch.Value, handshakeToken);
            }
            else if(this.streamSubscriptionObserver != null)
            {
                IStreamProvider sp = this.providerRuntime.ServiceProvider.GetServiceByName<IStreamProvider>(streamId.ProviderName);
                if(sp == null)
                {
                    logger.Warn((int)(ErrorCode.StreamProvider_NoStreamForBatch), $"{providerRuntime.ExecutingEntityIdentity()} got an item for stream provider {streamId.ProviderName}, but that provider is not configured. Dropping message.");
                    return default(StreamHandshakeToken);
                }
                IStreamSubscriptionHandle handle = CreateHandle(subscriptionId, streamId);
                await this.streamSubscriptionObserver.OnSubscribed(handle);
                // check if an observer were attached after handling the new subscription, deliver on it if attached
                if (allStreamObservers.TryGetValue(subscriptionId, out observer))
                {
                    return await observer.DeliverBatch(batch.Value, handshakeToken);
                }
            }

            logger.Warn((int)(ErrorCode.StreamProvider_NoStreamForBatch), "{0} got an item for subscription {1}, but I don't have any subscriber for that stream. Dropping on the floor.",
                providerRuntime.ExecutingEntityIdentity(), subscriptionId);
            // We got an item when we don't think we're the subscriber. This is a normal race condition.
            // We can drop the item on the floor, or pass it to the rendezvous, or ...
            return default(StreamHandshakeToken);
        }

        public Task ErrorInStream(GuidId subscriptionId, Exception exc)
        {
            if (logger.IsVerbose3) logger.Verbose3("ErrorInStream {0} for subscription {1}", exc, subscriptionId);

            if (allStreamObservers.TryGetValue(subscriptionId, out IStreamObserver observer))
                return observer.ErrorInStream(exc);

            logger.Warn((int)(ErrorCode.StreamProvider_NoStreamForItem), "{0} got an Error for subscription {1}, but I don't have any subscriber for that stream. Dropping on the floor.",
                providerRuntime.ExecutingEntityIdentity(), subscriptionId);
            // We got an item when we don't think we're the subscriber. This is a normal race condition.
            // We can drop the item on the floor, or pass it to the rendezvous, or ...
            return Task.CompletedTask;
        }

        public Task<StreamHandshakeToken> GetSequenceToken(GuidId subscriptionId)
        {
            return Task.FromResult(allStreamObservers.TryGetValue(subscriptionId, out IStreamObserver observer) ? observer.GetSequenceToken() : null);
        }

        internal void SetObserver<T>(IStreamSubscriptionHandle subscriptionHandle, IAsyncObserver<T> observer, StreamSequenceToken token)
        {
            if (null == subscriptionHandle) throw new ArgumentNullException(nameof(subscriptionHandle));
            if (null == observer) throw new ArgumentNullException(nameof(observer));

            try
            {
                var streamObserver = new StreamObserver<T>(subscriptionHandle, observer, IsRewindable(subscriptionHandle.ProviderName), token);
                allStreamObservers.AddOrUpdate(subscriptionHandle.SubscriptionId, streamObserver, (key, old) => streamObserver);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.StreamProvider_AddObserverException,
                    $"{providerRuntime.ExecutingEntityIdentity()} StreamConsumerExtension.AddObserver({subscriptionHandle}) caugth exception.", exc);
                throw;
            }
        }

        internal bool RemoveObserver(GuidId subscriptionId)
        {
            return allStreamObservers.TryRemove(subscriptionId, out IStreamObserver ignore);
        }

        private bool IsRewindable(string providerName)
        {
            IInternalStreamProvider sp = this.rewindablityCache.GetOrAdd(providerName, name => GetInternalStreamProvider(name));
            return sp.IsRewindable;
        }

        private IStreamPubSub GetPubSub(string providerName)
        {
            IInternalStreamProvider sp = this.rewindablityCache.GetOrAdd(providerName, name => GetInternalStreamProvider(name));
            return sp.StreamPubSub;
        }

        private IInternalStreamProvider GetInternalStreamProvider(string name)
        {
            return this.providerRuntime.ServiceProvider.GetServiceByName<IStreamProvider>(name) 
                as IInternalStreamProvider
                ?? throw new InvalidOperationException($"Stream provider {name} is not configured.");
        }

        internal int DiagCountStreamObservers<T>(StreamId streamId)
        {
            if (this.allStreamObservers.Count == 0) return 0;
            return allStreamObservers.Values.Count(o =>
                o.Handle.ProviderName == streamId.ProviderName &&
                o.Handle.StreamIdentity.Guid == streamId.Guid &&
                o.Handle.StreamIdentity.Namespace == streamId.Namespace);
        }

        internal IList<IStreamSubscriptionHandle> GetAllStreamHandles(StreamId streamId)
        {
            return allStreamObservers.Values.Where(o =>
                o.Handle.ProviderName == streamId.ProviderName &&
                o.Handle.StreamIdentity.Guid == streamId.Guid &&
                o.Handle.StreamIdentity.Namespace == streamId.Namespace)
                .Select(o => o.Handle)
                .ToList<IStreamSubscriptionHandle>();
        }

        public IStreamSubscriptionHandle CreateHandle(GuidId subscriptionId, StreamId streamId)
        {
            return new StreamSubscriptionHandle(subscriptionId, streamId.ProviderName, streamId, GetPubSub(streamId.ProviderName), this);
        }
    }
}
