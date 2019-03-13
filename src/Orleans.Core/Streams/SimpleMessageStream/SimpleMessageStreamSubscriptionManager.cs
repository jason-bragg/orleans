using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Streams;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace Orleans.Providers.Streams.SimpleMessageStream
{
    internal class SimpleMessageStreamSubscriptionManager
    {
        private readonly StreamId streamId;
        private readonly StreamConsumerExtensionCollection consumers;
        private readonly IStreamProviderRuntime     providerRuntime;
        private readonly IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar;
        private readonly bool                       fireAndForgetDelivery;
        private readonly bool                       optimizeForImmutableData;
        private readonly ILogger                    logger;
        private readonly ILoggerFactory             loggerFactory;

        internal SimpleMessageStreamSubscriptionManager(StreamId streamId, IStreamProviderRuntime providerRt, IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar, ILoggerFactory loggerFactory, bool fireAndForget, bool optimizeForImmutable)
        {
            this.streamId = streamId;
            providerRuntime = providerRt;
            this.registrar = registrar;
            fireAndForgetDelivery = fireAndForget;
            optimizeForImmutableData = optimizeForImmutable;
            consumers = new StreamConsumerExtensionCollection(registrar, loggerFactory);
            logger = loggerFactory.CreateLogger<SimpleMessageStreamSubscriptionManager>();
            this.loggerFactory = loggerFactory;
        }

        internal void UpdateSubscribers(IList<StreamSubscription<Guid>> subscribers)
        {
            IList<Guid> subscriptions = subscribers.Select(s => s.SubscriptionId).ToList();
            if (logger.IsEnabled(LogLevel.Debug))
                logger.Debug("{0} AddSubscribers {1} for stream {2}", providerRuntime.ExecutingEntityIdentity(), Utils.EnumerableToString(subscriptions), streamId);
            
            foreach (var subscriber in subscribers)
            {
                var subscriptionId = GuidId.GetGuidId(subscriber.SubscriptionId);
                if (!consumers.Contains(subscriptionId))
                {
                    AddSubscriber(subscriptionId, subscriber.Consumer.AsReference<IStreamConsumerExtension>());
                }
            }

            foreach (GuidId subscriptionId in consumers.Subscriptions())
            {
                if (!subscriptions.Contains(subscriptionId.Guid))
                {
                    RemoveSubscriber(subscriptionId);
                }
            }
        }

        internal Task DeliverItem(object item)
        {
            return consumers.DeliverItem(streamId, item, fireAndForgetDelivery, optimizeForImmutableData);
        }

        internal Task CompleteStream()
        {
            return consumers.CompleteStream(streamId, fireAndForgetDelivery);
        }

        internal Task ErrorInStream(Exception exc)
        {
            return consumers.ErrorInStream(streamId, exc, fireAndForgetDelivery);
        }

        private void AddSubscriber(GuidId subscriptionId, IStreamConsumerExtension streamConsumer)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.Debug("{0} AddSubscriber {1} for stream {2}", providerRuntime.ExecutingEntityIdentity(), streamConsumer, streamId);
            }

            consumers.AddRemoteSubscriber(subscriptionId, streamConsumer);
        }

        private void RemoveSubscriber(GuidId subscriptionId)
        {
            if (logger.IsEnabled(LogLevel.Debug))
            {
                logger.Debug("{0} RemoveSubscription {1}", providerRuntime.ExecutingEntityIdentity(),
                    subscriptionId);
            }

            consumers.RemoveRemoteSubscriber(subscriptionId);
        }

        [Serializable]
        internal class StreamConsumerExtensionCollection
        {
            private readonly ConcurrentDictionary<GuidId, IStreamConsumerExtension> consumers;
            private readonly IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar;
            private readonly ILogger logger;

            internal StreamConsumerExtensionCollection(IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar, ILoggerFactory loggerFactory)
            {
                this.registrar = registrar;
                this.logger = loggerFactory.CreateLogger<StreamConsumerExtensionCollection>();
                consumers = new ConcurrentDictionary<GuidId, IStreamConsumerExtension>();
            }

            internal void AddRemoteSubscriber(GuidId subscriptionId, IStreamConsumerExtension streamConsumer)
            {
                consumers.TryAdd(subscriptionId, streamConsumer);
            }

            internal void RemoveRemoteSubscriber(GuidId subscriptionId)
            {
                consumers.TryRemove(subscriptionId, out IStreamConsumerExtension ignore);
                if (consumers.Count == 0)
                {
                    // Unsubscribe from PubSub?
                }
            }

            internal Task DeliverItem(StreamId streamId, object item, bool fireAndForgetDelivery, bool optimizeForImmutableData)
            {
                var tasks = fireAndForgetDelivery ? null : new List<Task>();
                foreach (KeyValuePair<GuidId, IStreamConsumerExtension> subscriptionKvp in consumers)
                {
                    IStreamConsumerExtension remoteConsumer = subscriptionKvp.Value;

                    Task task = DeliverToRemote(remoteConsumer, streamId, subscriptionKvp.Key, item, optimizeForImmutableData, fireAndForgetDelivery);
                    if (fireAndForgetDelivery) task.Ignore();
                    else tasks.Add(task);
                }
                // If there's no subscriber, presumably we just drop the item on the floor
                return fireAndForgetDelivery ? Task.CompletedTask : Task.WhenAll(tasks);
            }

            private async Task DeliverToRemote(IStreamConsumerExtension remoteConsumer, StreamId streamId, GuidId subscriptionId, object item, bool optimizeForImmutableData, bool fireAndForgetDelivery)
            {
                try
                {
                    if (optimizeForImmutableData)
                        await remoteConsumer.DeliverImmutable(subscriptionId, streamId, new Immutable<object>(item), null, null);
                    else
                        await remoteConsumer.DeliverMutable(subscriptionId, streamId, item, null, null);
                }
                catch (ClientNotAvailableException)
                {
                    if (consumers.TryRemove(subscriptionId, out IStreamConsumerExtension discard))
                    {
                        this.registrar.Unregister(streamId, subscriptionId.Guid).Ignore();
                        logger.Warn(ErrorCode.Stream_ConsumerIsDead,
                            "Consumer {0} on stream {1} is no longer active - permanently removing Consumer.", remoteConsumer, streamId);
                    }
                }
                catch(Exception ex)
                {
                    if (!fireAndForgetDelivery)
                    {
                        throw;
                    }
                    this.logger.LogWarning(ex, "Failed to deliver message to consumer on {SubscriptionId} for stream {StreamId}.", subscriptionId, streamId);
                }
            }

            internal Task CompleteStream(StreamId streamId, bool fireAndForgetDelivery)
            {
                var tasks = fireAndForgetDelivery ? null : new List<Task>();
                foreach (KeyValuePair<GuidId, IStreamConsumerExtension> kvp in consumers)
                {
                    IStreamConsumerExtension remoteConsumer = kvp.Value;
                    GuidId subscriptionId = kvp.Key;
                    Task task = NotifyComplete(remoteConsumer, subscriptionId, streamId, fireAndForgetDelivery);
                    if (fireAndForgetDelivery) task.Ignore();
                    else tasks.Add(task);
                }
                // If there's no subscriber, presumably we just drop the item on the floor
                return fireAndForgetDelivery ? Task.CompletedTask : Task.WhenAll(tasks);
            }

            private async Task NotifyComplete(IStreamConsumerExtension remoteConsumer, GuidId subscriptionId, StreamId streamId, bool fireAndForgetDelivery)
            {
                try
                {
                    await remoteConsumer.CompleteStream(subscriptionId);
                } catch(Exception ex)
                {
                    if (!fireAndForgetDelivery)
                    {
                        throw;
                    }
                    this.logger.LogWarning(ex, "Failed to notify consumer of stream completion on {SubscriptionId} for stream {StreamId}.", subscriptionId, streamId);
                }
            }

            internal Task ErrorInStream(StreamId streamId, Exception exc, bool fireAndForgetDelivery)
            {
                var tasks = fireAndForgetDelivery ? null : new List<Task>();
                foreach (KeyValuePair<GuidId, IStreamConsumerExtension> kvp in consumers)
                {
                    IStreamConsumerExtension remoteConsumer = kvp.Value;
                    GuidId subscriptionId = kvp.Key;
                    Task task = NotifyError(remoteConsumer, subscriptionId, exc, streamId, fireAndForgetDelivery);
                    if (fireAndForgetDelivery) task.Ignore();
                    else tasks.Add(task);
                }
                // If there's no subscriber, presumably we just drop the item on the floor
                return fireAndForgetDelivery ? Task.CompletedTask : Task.WhenAll(tasks);
            }

            internal bool Contains(GuidId subscriptionId)
            {
                return consumers.ContainsKey(subscriptionId);
            }

            internal IEnumerable<GuidId> Subscriptions()
            {
                return consumers.Keys;
            }


            private async Task NotifyError(IStreamConsumerExtension remoteConsumer, GuidId subscriptionId, Exception exc, StreamId streamId, bool fireAndForgetDelivery)
            {
                try
                {
                    await remoteConsumer.ErrorInStream(subscriptionId, exc);
                }
                catch (Exception ex)
                {
                    if (!fireAndForgetDelivery)
                    {
                        throw;
                    }
                    this.logger.LogWarning(ex, "Failed to notify consumer of stream error on {SubscriptionId} for stream {StreamId}. Error: {ErrorException}", subscriptionId, streamId, exc);
                }
            }
        }
    }
}
