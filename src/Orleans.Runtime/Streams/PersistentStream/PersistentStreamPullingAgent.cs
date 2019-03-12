using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Streams
{
    internal class PersistentStreamPullingAgent : SystemTarget, IPersistentStreamPullingAgent
    {
        private static readonly IBackoffProvider DeliveryBackoffProvider = new ExponentialBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(1));
        private static readonly IBackoffProvider ReadLoopBackoff = new ExponentialBackoff(TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(1));
        private const int ReadLoopRetryMax = 6;
        private const int StreamInactivityCheckFrequency = 10;
        private readonly string streamProviderName;
        private readonly IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar;
        private readonly IStreamSubscriptionManifest<Guid, IStreamIdentity> manifest;
        private readonly Dictionary<IStreamIdentity, StreamConsumerCollection> pubSubCache;
        private readonly SafeRandom safeRandom;
        private readonly StreamPullingAgentOptions options;
        private readonly ILogger logger;
        private readonly CounterStatistic numReadMessagesCounter;
        private readonly CounterStatistic numSentMessagesCounter;
        private int numMessages;

        private IQueueAdapter queueAdapter;
        private IQueueCache queueCache;
        private IQueueAdapterReceiver receiver;
        private IStreamFailureHandler streamFailureHandler;
        private DateTime lastTimeCleanedPubSubCache;
        private IDisposable timer;

        internal readonly QueueId QueueId;
        private Task receiverInitTask;
        private bool IsShutdown => timer == null;
        private string StatisticUniquePostfix => streamProviderName + "." + QueueId;

        internal PersistentStreamPullingAgent(
            GrainId id,
            string strProviderName,
            IStreamProviderRuntime runtime,
            ILoggerFactory loggerFactory,
            IStreamSubscriptionRegistrar<Guid, IStreamIdentity> subscriptionRegistrar,
            IStreamSubscriptionManifest<Guid, IStreamIdentity> manifest,
            QueueId queueId,
            StreamPullingAgentOptions options)
            : base(id, runtime.ExecutingSiloAddress, true, loggerFactory)
        {
            if (runtime == null) throw new ArgumentNullException("runtime", "PersistentStreamPullingAgent: runtime reference should not be null");
            if (strProviderName == null) throw new ArgumentNullException("strProviderName", "PersistentStreamPullingAgent: strProviderName should not be null");

            QueueId = queueId;
            streamProviderName = strProviderName;
            this.subscriptionRegistrar = subscriptionRegistrar;
            this.manifest = manifest;
            pubSubCache = new Dictionary<IStreamIdentity, StreamConsumerCollection>(StreamIdentityEqualityComparer.Instance);
            safeRandom = new SafeRandom();
            this.options = options;
            numMessages = 0;

            logger = runtime.ServiceProvider.GetRequiredService<ILoggerFactory>().CreateLogger($"{this.GetType().Namespace}.{((ISystemTargetBase)this).GrainId}.{streamProviderName}");
            logger.Info(ErrorCode.PersistentStreamPullingAgent_01,
                "Created {0} {1} for Stream Provider {2} on silo {3} for Queue {4}.",
                GetType().Name, ((ISystemTargetBase)this).GrainId.ToDetailedString(), streamProviderName, Silo, QueueId.ToStringWithHashCode());
            numReadMessagesCounter = CounterStatistic.FindOrCreate(new StatisticName(StatisticNames.STREAMS_PERSISTENT_STREAM_NUM_READ_MESSAGES, StatisticUniquePostfix));
            numSentMessagesCounter = CounterStatistic.FindOrCreate(new StatisticName(StatisticNames.STREAMS_PERSISTENT_STREAM_NUM_SENT_MESSAGES, StatisticUniquePostfix));
            // TODO: move queue cache size statistics tracking into queue cache implementation once Telemetry APIs and LogStatistics have been reconciled.
            //IntValueStatistic.FindOrCreate(new StatisticName(StatisticNames.STREAMS_PERSISTENT_STREAM_QUEUE_CACHE_SIZE, statUniquePostfix), () => queueCache != null ? queueCache.Size : 0);
        }

        /// <summary>
        /// Take responsibility for a new queues that was assigned to me via a new range.
        /// We first store the new queue in our internal data structure, try to initialize it and start a pumping timer.
        /// ERROR HANDLING:
        ///     The responsibility to handle initialization and shutdown failures is inside the INewQueueAdapterReceiver code.
        ///     The agent will call Initialize once and log an error. It will not call initialize again.
        ///     The receiver itself may attempt later to recover from this error and do initialization again. 
        ///     The agent will assume initialization has succeeded and will subsequently start calling pumping receive.
        ///     Same applies to shutdown.
        /// </summary>
        /// <param name="qAdapter"></param>
        /// <param name="queueAdapterCache"></param>
        /// <param name="failureHandler"></param>
        /// <returns></returns>
        public Task Initialize(Immutable<IQueueAdapter> qAdapter, Immutable<IQueueAdapterCache> queueAdapterCache, Immutable<IStreamFailureHandler> failureHandler)
        {
            if (qAdapter.Value == null) throw new ArgumentNullException("qAdapter", "Init: queueAdapter should not be null");
            if (failureHandler.Value == null) throw new ArgumentNullException("failureHandler", "Init: streamDeliveryFailureHandler should not be null");
            return OrleansTaskExtentions.WrapInTask(() => InitializeInternal(qAdapter.Value, queueAdapterCache.Value, failureHandler.Value));
        }

        private void InitializeInternal(IQueueAdapter qAdapter, IQueueAdapterCache queueAdapterCache, IStreamFailureHandler failureHandler)
        {
            logger.Info(ErrorCode.PersistentStreamPullingAgent_02, "Init of {0} {1} on silo {2} for queue {3}.",
                GetType().Name, ((ISystemTargetBase)this).GrainId.ToDetailedString(), Silo, QueueId.ToStringWithHashCode());

            // Remove cast once we cleanup
            queueAdapter = qAdapter;
            streamFailureHandler = failureHandler;
            lastTimeCleanedPubSubCache = DateTime.UtcNow;

            try
            {
                receiver = queueAdapter.CreateReceiver(QueueId);
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.PersistentStreamPullingAgent_02, "Exception while calling IQueueAdapter.CreateNewReceiver.", exc);
                throw;
            }

            try
            {
                if (queueAdapterCache != null)
                {
                    queueCache = queueAdapterCache.CreateQueueCache(QueueId);
                }
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.PersistentStreamPullingAgent_23, "Exception while calling IQueueAdapterCache.CreateQueueCache.", exc);
                throw;
            }

            try
            {
                receiverInitTask = OrleansTaskExtentions.SafeExecute(() => receiver.Initialize(this.options.InitQueueTimeout))
                    .LogException(logger, ErrorCode.PersistentStreamPullingAgent_03, $"QueueAdapterReceiver {QueueId.ToStringWithHashCode()} failed to Initialize.");
                receiverInitTask.Ignore();
            }
            catch
            {
                // Just ignore this exception and proceed as if Initialize has succeeded.
                // We already logged individual exceptions for individual calls to Initialize. No need to log again.
            }
            // Setup a reader for a new receiver. 
            // Even if the receiver failed to initialise, treat it as OK and start pumping it. It's receiver responsibility to retry initialization.
            var randomTimerOffset = safeRandom.NextTimeSpan(this.options.GetQueueMsgsTimerPeriod);
            timer = RegisterTimer(AsyncTimerCallback, QueueId, randomTimerOffset, this.options.GetQueueMsgsTimerPeriod);

            IntValueStatistic.FindOrCreate(new StatisticName(StatisticNames.STREAMS_PERSISTENT_STREAM_PUBSUB_CACHE_SIZE, StatisticUniquePostfix), () => pubSubCache.Count);

            logger.Info((int)ErrorCode.PersistentStreamPullingAgent_04, "Taking queue {0} under my responsibility.", QueueId.ToStringWithHashCode());
        }

        public async Task Shutdown()
        {
            // Stop pulling from queues that are not in my range anymore.
            logger.Info(ErrorCode.PersistentStreamPullingAgent_05, "Shutdown of {0} responsible for queue: {1}", GetType().Name, QueueId.ToStringWithHashCode());

            if (timer != null)
            {
                IDisposable tmp = timer;
                timer = null;
                Utils.SafeExecute(tmp.Dispose, this.logger);
            }

            this.queueCache = null;

            Task localReceiverInitTask = receiverInitTask;
            if (localReceiverInitTask != null)
            {
                try
                {
                    await localReceiverInitTask;
                    receiverInitTask = null;
                }
                catch (Exception)
                {
                    receiverInitTask = null;
                    // squelch
                }
            }

            try
            {
                IQueueAdapterReceiver localReceiver = this.receiver;
                this.receiver = null;
                if (localReceiver != null)
                {
                    var task = OrleansTaskExtentions.SafeExecute(() => localReceiver.Shutdown(this.options.InitQueueTimeout));
                    task = task.LogException(logger, ErrorCode.PersistentStreamPullingAgent_07,
                        $"QueueAdapterReceiver {QueueId} failed to Shutdown.");
                    await task;
                }
            }
            catch
            {
                // Just ignore this exception and proceed as if Shutdown has succeeded.
                // We already logged individual exceptions for individual calls to Shutdown. No need to log again.
            }

            var unregisterTasks = new List<Task>();
            var meAsStreamProducer = this.AsReference<IStreamProducerExtension>();
            foreach (var tuple in pubSubCache)
            {
                tuple.Value.DisposeAll(logger);
                var streamId = tuple.Key;
                logger.Info(ErrorCode.PersistentStreamPullingAgent_06, "Unregister PersistentStreamPullingAgent Producer for stream {0}.", streamId);
            }

            try
            {
                await Task.WhenAll(unregisterTasks);
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.PersistentStreamPullingAgent_08,
                    "Failed to unregister myself as stream producer to some streams that used to be in my responsibility.", exc);
            }
            pubSubCache.Clear();
            IntValueStatistic.Delete(new StatisticName(StatisticNames.STREAMS_PERSISTENT_STREAM_PUBSUB_CACHE_SIZE, StatisticUniquePostfix));
            //IntValueStatistic.Delete(new StatisticName(StatisticNames.STREAMS_PERSISTENT_STREAM_QUEUE_CACHE_SIZE, StatisticUniquePostfix));
        }

        // Called by rendezvous when new remote subscriber subscribes to this stream.
        private async Task AddSubscriber(
            StreamConsumerCollection streamDataCollection,
            Guid subscriptionId,
            IStreamIdentity streamId,
            IStreamConsumerExtension streamConsumer,
            StreamSequenceToken cacheToken,
            DateTime nowUtc)
        {
            if (IsShutdown) return;

            StreamConsumerData data = streamDataCollection.AddConsumer(subscriptionId, streamId, streamConsumer, nowUtc);

            if (await DoHandshakeWithConsumer(data, cacheToken))
            {
                if (data.State == StreamConsumerDataState.Inactive)
                    RunConsumerCursor(data).Ignore(); // Start delivering events if not actively doing so
            }
        }

        private async Task<bool> DoHandshakeWithConsumer(
            StreamConsumerData consumerData,
            StreamSequenceToken cacheToken)
        {
            StreamHandshakeToken requestedHandshakeToken = null;
            // if not cache, then we can't get cursor and there is no reason to ask consumer for token.
            if (queueCache != null)
            {
                Exception exceptionOccured = null;
                try
                {
                    requestedHandshakeToken = await AsyncExecutorWithRetries.ExecuteWithRetries(
                         i => consumerData.StreamConsumer.GetSequenceToken(consumerData.SubscriptionId),
                         AsyncExecutorWithRetries.INFINITE_RETRIES,
                         (exception, i) => !(exception is ClientNotAvailableException) && !this.IsShutdown,
                         this.options.MaxEventDeliveryTime,
                         DeliveryBackoffProvider);

                    if (requestedHandshakeToken != null)
                    {
                        consumerData.SafeDisposeCursor(logger);
                        consumerData.Cursor = queueCache.GetCacheCursor(consumerData.StreamId, requestedHandshakeToken.Token);
                    }
                    else
                    {
                        if (consumerData.Cursor == null) // if the consumer did not ask for a specific token and we already have a cursor, just keep using it.
                            consumerData.Cursor = queueCache.GetCacheCursor(consumerData.StreamId, cacheToken);
                    }
                }
                catch (Exception exception)
                {
                    exceptionOccured = exception;
                }
                if (exceptionOccured != null)
                {
                    bool faultedSubscription = await ErrorProtocol(consumerData, exceptionOccured, false, null, requestedHandshakeToken?.Token);
                    if (faultedSubscription) return false;
                }
            }
            consumerData.LastToken = requestedHandshakeToken; // use what ever the consumer asked for as LastToken for next handshake (even if null was asked for).
            // if we don't yet have a cursor (had errors in the handshake or data not available exc), get a cursor at the event that triggered that consumer subscription.
            if (consumerData.Cursor == null && queueCache != null)
            {
                try
                {
                    consumerData.Cursor = queueCache.GetCacheCursor(consumerData.StreamId, cacheToken);
                }
                catch (Exception)
                {
                    consumerData.Cursor = queueCache.GetCacheCursor(consumerData.StreamId, null); // just in case last GetCacheCursor failed.
                }
            }
            return true;
        }

        private async Task AsyncTimerCallback(object state)
        {
            var queueId = (QueueId)state;
            try
            {
                Task localReceiverInitTask = receiverInitTask;
                if (localReceiverInitTask != null)
                {
                    await localReceiverInitTask;
                    receiverInitTask = null;
                }

                if (IsShutdown) return; // timer was already removed, last tick

                // loop through the queue until it is empty.
                while (!IsShutdown) // timer will be set to null when we are asked to shudown. 
                {
                    int maxCacheAddCount = queueCache?.GetMaxAddCount() ?? QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG;
                    if (maxCacheAddCount != QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG && maxCacheAddCount <= 0)
                        return;

                    // If read succeeds and there is more data, we continue reading.
                    // If read succeeds and there is no more data, we break out of loop
                    // If read fails, we retry 6 more times, with backoff policy.
                    //    we log each failure as warnings. After 6 times retry if still fail, we break out of loop and log an error
                    bool moreData = await AsyncExecutorWithRetries.ExecuteWithRetries(
                        i => ReadFromQueue(queueId, receiver, maxCacheAddCount),
                        ReadLoopRetryMax,
                        ReadLoopRetryExceptionFilter,
                        Constants.INFINITE_TIMESPAN,
                        ReadLoopBackoff);
                    if (!moreData)
                        return;
                }
            }
            catch (Exception exc)
            {
                receiverInitTask = null;
                logger.Error(ErrorCode.PersistentStreamPullingAgent_12, $"Giving up reading from queue {queueId} after retry attempts {ReadLoopRetryMax}", exc);
            }
        }

        private bool ReadLoopRetryExceptionFilter(Exception e, int retryCounter)
        {
            this.logger.Warn(ErrorCode.PersistentStreamPullingAgent_12, $"Exception while retrying the {retryCounter}th time reading from queue {this.QueueId}", e);
            return !IsShutdown;
        }

        /// <summary>
        /// Read from queue.
        /// Returns true, if data was read, false if it was not
        /// </summary>
        /// <param name="myQueueId"></param>
        /// <param name="rcvr"></param>
        /// <param name="maxCacheAddCount"></param>
        /// <returns></returns>
        private async Task<bool> ReadFromQueue(QueueId myQueueId, IQueueAdapterReceiver rcvr, int maxCacheAddCount)
        {
            if (rcvr == null)
            {
                return false;
            }

            var nowUtc = DateTime.UtcNow;
            // Try to cleanup the pubsub cache at the cadence of 10 times in the configurable StreamInactivityPeriod.
            if ((nowUtc - lastTimeCleanedPubSubCache) >= this.options.StreamInactivityPeriod.Divide(StreamInactivityCheckFrequency))
            {
                lastTimeCleanedPubSubCache = nowUtc;
                CleanupPubSubCache(nowUtc);
            }

            if (queueCache != null)
            {
                IList<IBatchContainer> purgedItems;
                if (queueCache.TryPurgeFromCache(out purgedItems))
                {
                    try
                    {
                        await rcvr.MessagesDeliveredAsync(purgedItems);
                    }
                    catch (Exception exc)
                    {
                        logger.Warn(ErrorCode.PersistentStreamPullingAgent_27,
                            $"Exception calling MessagesDeliveredAsync on queue {myQueueId}. Ignoring.", exc);
                    }
                }
            }

            if (queueCache != null && queueCache.IsUnderPressure())
            {
                // Under back pressure. Exit the loop. Will attempt again in the next timer callback.
                logger.Info((int)ErrorCode.PersistentStreamPullingAgent_24, "Stream cache is under pressure. Backing off.");
                return false;
            }

            // Retrieve one multiBatch from the queue. Every multiBatch has an IEnumerable of IBatchContainers, each IBatchContainer may have multiple events.
            IList<IBatchContainer> multiBatch = await rcvr.GetQueueMessagesAsync(maxCacheAddCount);

            if (multiBatch == null || multiBatch.Count == 0) return false; // queue is empty. Exit the loop. Will attempt again in the next timer callback.

            queueCache?.AddToCache(multiBatch);
            numMessages += multiBatch.Count;
            numReadMessagesCounter.IncrementBy(multiBatch.Count);
            if (logger.IsEnabled(LogLevel.Trace)) logger.Trace(ErrorCode.PersistentStreamPullingAgent_11, "Got {0} messages from queue {1}. So far {2} msgs from this queue.",
                multiBatch.Count, myQueueId.ToStringWithHashCode(), numMessages);

            foreach (var group in
                multiBatch
                .Where(m => m != null)
                .GroupBy(container => new Tuple<Guid, string>(container.StreamGuid, container.StreamNamespace)))
            {
                StreamId streamId = StreamId.GetStreamId(group.Key.Item1, queueAdapter.Name, group.Key.Item2);
                StreamSequenceToken startToken = group.First().SequenceToken;
                if (pubSubCache.TryGetValue(streamId, out StreamConsumerCollection streamData))
                {
                    RefreshStream(streamData, startToken, nowUtc).Ignore();
                }
                else
                {
                    RegisterStream(streamId, startToken, nowUtc).Ignore(); // if this is a new stream register as producer of stream in pub sub system
                }
            }
            return true;
        }

        private async Task RefreshStream(StreamConsumerCollection streamData, StreamSequenceToken startToken, DateTime nowUtc)
        {
            streamData.RefreshActivity(nowUtc);
            if (streamData.StreamRegistered)
            {
                if (!streamData.Subscriptions.NextAsync.IsCompleted)
                {
                    // if this is an existing stream and there are no subscription changes, start any inactive cursors
                    StartInactiveCursors(streamData, startToken); 
                }
                else
                {
                    // if this is an existing stream and there are subscription changes, handle subscription changes
                    await HandleSubscriptionChanges(streamData, startToken, nowUtc);
                }
            }
            else
            {
                if (this.logger.IsEnabled(LogLevel.Debug))
                    this.logger.LogDebug("Pulled new messages in stream {StreamId} from the queue, but pulling agent haven't succeeded in" +
                                           "RegisterStream yet, will start deliver on this stream after RegisterStream succeeded", streamData.StreamId);
            }
        }

        private async Task HandleSubscriptionChanges(StreamConsumerCollection streamData, StreamSequenceToken startToken, DateTime nowUtc)
        {
            if (!streamData.Subscriptions.NextAsync.IsCompleted)
                return;

            // update subscriptions
            streamData.Subscriptions = await streamData.Subscriptions.NextAsync;

            // remove dropped subscriptions
            foreach (StreamConsumerData consumer in streamData.AllConsumers())
            {
                if (!streamData.Subscriptions.Value.Any(s => s.SubscriptionId.Equals(consumer.SubscriptionId)))
                {
                    streamData.RemoveConsumer(consumer.SubscriptionId.Guid, this.logger);
                }
            }

            StartInactiveCursors(streamData, startToken); // if this is an existing stream, start any inactive cursors

            List<Task> pending = new List<Task>();
            // add new subscriptions
            foreach (StreamSubscription<Guid> subscription in streamData.Subscriptions.Value)
            {
                if(!streamData.TryGetConsumer(subscription.SubscriptionId, out StreamConsumerData consumer))
                {
                    pending.Add(AddSubscriber(streamData, subscription.SubscriptionId, streamData.StreamId, subscription.Consumer.AsReference<IStreamConsumerExtension>(), startToken, nowUtc));
                }
            }
            if(pending.Count == 0)
            {
                await Task.WhenAll(pending);
            }
        }

        private void CleanupPubSubCache(DateTime now)
        {
            if (pubSubCache.Count == 0) return;
            var toRemove = pubSubCache.Where(pair => pair.Value.IsInactive(now, this.options.StreamInactivityPeriod))
                         .ToList();
            toRemove.ForEach(tuple =>
            {
                pubSubCache.Remove(tuple.Key);
                tuple.Value.DisposeAll(logger);
            });
        }

        private async Task RegisterStream(StreamId streamId, StreamSequenceToken firstToken, DateTime nowUtc)
        {
            var streamData = new StreamConsumerCollection(streamId, nowUtc);
            pubSubCache.Add(streamId, streamData);
            // Create a fake cursor to point into a cache.
            // That way we will not purge the event from the cache, until we talk to pub sub.
            // This will help ensure the "casual consistency" between pre-existing subscripton (of a potentially new already subscribed consumer) 
            // and later production.
            var pinCursor = queueCache?.GetCacheCursor(streamId, firstToken);

            try
            {
                await MonitorStreamSubscriptions(streamData, firstToken, nowUtc);
            }
            finally
            {
                // Cleanup the fake pinning cursor.
                pinCursor?.Dispose();
            }
        }

        private void StartInactiveCursors(StreamConsumerCollection streamData, StreamSequenceToken startToken)
        {
            foreach (StreamConsumerData consumerData in streamData.AllConsumers())
            {
                consumerData.Cursor?.Refresh(startToken);
                if (consumerData.State == StreamConsumerDataState.Inactive)
                {
                    // wake up inactive consumers
                    RunConsumerCursor(consumerData).Ignore();
                }
            }
        }

        private async Task RunConsumerCursor(StreamConsumerData consumerData)
        {
            try
            {
                // double check in case of interleaving
                if (consumerData.State == StreamConsumerDataState.Active ||
                    consumerData.Cursor == null) return;

                consumerData.State = StreamConsumerDataState.Active;
                while (consumerData.Cursor != null)
                {
                    IBatchContainer batch = null;
                    Exception exceptionOccured = null;
                    try
                    {
                        batch = GetBatchForConsumer(consumerData.Cursor, consumerData.StreamId);
                        if (batch == null)
                        {
                            break;
                        }
                    }
                    catch (Exception exc)
                    {
                        exceptionOccured = exc;
                        consumerData.SafeDisposeCursor(logger);
                        consumerData.Cursor = queueCache.GetCacheCursor(consumerData.StreamId, null);
                    }

                    try
                    {
                        numSentMessagesCounter.Increment();
                        if (batch != null)
                        {
                            StreamHandshakeToken newToken = await AsyncExecutorWithRetries.ExecuteWithRetries(
                                i => DeliverBatchToConsumer(consumerData, batch),
                                AsyncExecutorWithRetries.INFINITE_RETRIES,
                                (exception, i) => !(exception is ClientNotAvailableException) && !this.IsShutdown,
                                this.options.MaxEventDeliveryTime,
                                DeliveryBackoffProvider);
                            if (newToken != null)
                            {
                                consumerData.LastToken = newToken;
                                IQueueCacheCursor newCursor = queueCache.GetCacheCursor(consumerData.StreamId, newToken.Token);
                                consumerData.SafeDisposeCursor(logger);
                                consumerData.Cursor = newCursor;
                            }
                        }
                    }
                    catch (Exception exc)
                    {
                        consumerData.Cursor?.RecordDeliveryFailure();
                        var message =
                            $"Exception while trying to deliver msgs to stream {consumerData.StreamId} in PersistentStreamPullingAgentGrain.RunConsumerCursor";
                        logger.Error(ErrorCode.PersistentStreamPullingAgent_14, message, exc);
                        exceptionOccured = exc is ClientNotAvailableException
                            ? exc
                            : new StreamEventDeliveryFailureException(this.streamProviderName, consumerData.StreamId);
                    }
                    // if we failed to deliver a batch
                    if (exceptionOccured != null)
                    {
                        bool faultedSubscription = await ErrorProtocol(consumerData, exceptionOccured, true, batch, batch?.SequenceToken);
                        if (faultedSubscription) return;
                    }
                }
                consumerData.State = StreamConsumerDataState.Inactive;
            }
            catch (Exception exc)
            {
                // RunConsumerCursor is fired with .Ignore so we should log if anything goes wrong, because there is no one to catch the exception
                logger.Error(ErrorCode.PersistentStreamPullingAgent_15, "Ignored RunConsumerCursor Error", exc);
                consumerData.State = StreamConsumerDataState.Inactive;
                throw;
            }
        }

        private IBatchContainer GetBatchForConsumer(IQueueCacheCursor cursor, IStreamIdentity streamId)
        {
            if (this.options.BatchContainerBatchSize <= 1)
            {
                Exception ignore;

                if (!cursor.MoveNext())
                {
                    return null;
                }

                return cursor.GetCurrent(out ignore);
            }
            else if (this.options.BatchContainerBatchSize > 1)
            {
                Exception ignore;
                int i = 0;
                var batchContainers = new List<IBatchContainer>();

                while (i < this.options.BatchContainerBatchSize)
                {
                    if (!cursor.MoveNext())
                    {
                        break;
                    }

                    batchContainers.Add(cursor.GetCurrent(out ignore));
                    i++;
                }

                if (i == 0)
                {
                    return null;
                }

                return new BatchContainerBatch(batchContainers);
            }

            return null;
        }

        private async Task<StreamHandshakeToken> DeliverBatchToConsumer(StreamConsumerData consumerData, IBatchContainer batch)
        {
            try
            {
                StreamHandshakeToken newToken = await ContextualizedDeliverBatchToConsumer(consumerData, batch);
                consumerData.LastToken = StreamHandshakeToken.CreateDeliveyToken(batch.SequenceToken); // this is the currently delivered token
                return newToken;
            }
            catch (Exception ex)
            {
                this.logger.LogWarning(ex, "Failed to deliver message to consumer on {SubscriptionId} for stream {StreamId}, may retry.", consumerData.SubscriptionId, consumerData.StreamId);
                throw;
            }
        }

        /// <summary>
        /// Add call context for batch delivery call, then clear context immediately, without giving up turn.
        /// </summary>
        private Task<StreamHandshakeToken> ContextualizedDeliverBatchToConsumer(StreamConsumerData consumerData, IBatchContainer batch)
        {
            bool isRequestContextSet = batch.ImportRequestContext();
            try
            {
                StreamId streamId = StreamId.GetStreamId(consumerData.StreamId.Guid, this.streamProviderName, consumerData.StreamId.Namespace);
                return consumerData.StreamConsumer.DeliverBatch(consumerData.SubscriptionId, streamId, batch.AsImmutable(), consumerData.LastToken);
            }
            finally
            {
                if (isRequestContextSet)
                {
                    // clear RequestContext before await!
                    RequestContext.Clear();
                }
            }
        }


        private static async Task DeliverErrorToConsumer(StreamConsumerData consumerData, Exception exc, IBatchContainer batch)
        {
            Task errorDeliveryTask;
            bool isRequestContextSet = batch != null && batch.ImportRequestContext();
            try
            {
                errorDeliveryTask = consumerData.StreamConsumer.ErrorInStream(consumerData.SubscriptionId, exc);
            }
            finally
            {
                if (isRequestContextSet)
                {
                    RequestContext.Clear(); // clear RequestContext before await!
                }
            }
            await errorDeliveryTask;
        }

        private async Task<bool> ErrorProtocol(StreamConsumerData consumerData, Exception exceptionOccured, bool isDeliveryError, IBatchContainer batch, StreamSequenceToken token)
        {
            // for loss of client, we just remove the subscription
            if (exceptionOccured is ClientNotAvailableException)
            {
                logger.Warn(ErrorCode.Stream_ConsumerIsDead,
                    "Consumer {0} on stream {1} is no longer active - permanently removing Consumer.", consumerData.StreamConsumer, consumerData.StreamId);
                this.subscriptionRegistrar.Unregister(consumerData.StreamId, consumerData.SubscriptionId.Guid).Ignore();
                return true;
            }

            // notify consumer about the error or that the data is not available.
            await OrleansTaskExtentions.ExecuteAndIgnoreException(
                () => DeliverErrorToConsumer(
                    consumerData, exceptionOccured, batch));
            // record that there was a delivery failure
            if (isDeliveryError)
            {
                await OrleansTaskExtentions.ExecuteAndIgnoreException(
                    () => streamFailureHandler.OnDeliveryFailure(
                        consumerData.SubscriptionId, streamProviderName, consumerData.StreamId, token));
            }
            else
            {
                await OrleansTaskExtentions.ExecuteAndIgnoreException(
                       () => streamFailureHandler.OnSubscriptionFailure(
                           consumerData.SubscriptionId, streamProviderName, consumerData.StreamId, token));
            }
            return false;
        }

        private static async Task<IAsyncLinkedListNode<IList<StreamSubscription<Guid>>>> MonitorSubscriptions(IStreamSubscriptionManifest<Guid, IStreamIdentity> manifest, StreamId streamId, string streamProviderName,
            IStreamProducerExtension meAsStreamProducer, ILogger logger)
        {
            try
            {
                return await manifest.MonitorSubscriptions(streamId);
            }
            catch (Exception e)
            {
                logger.Error(ErrorCode.PersistentStreamPullingAgent_17, $"MonitorSubscriptions failed due to {e}", e);
                throw e;
            }
        }

        private async Task MonitorStreamSubscriptions(StreamConsumerCollection streamData, StreamSequenceToken streamStartToken, DateTime nowUtc)
        {
            try
            {
                IStreamProducerExtension meAsStreamProducer = this.AsReference<IStreamProducerExtension>();
                IAsyncLinkedListNode<IList<StreamSubscription<Guid>>> subscriptions = null;
                await AsyncExecutorWithRetries.ExecuteWithRetries(
                                async i => {
                                    subscriptions = 
                                    await MonitorSubscriptions(this.manifest, streamData.StreamId, streamProviderName, meAsStreamProducer, logger); },
                                AsyncExecutorWithRetries.INFINITE_RETRIES,
                                (exception, i) => !IsShutdown,
                                Constants.INFINITE_TIMESPAN,
                                DeliveryBackoffProvider);

                streamData.Subscriptions = subscriptions;

                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug(ErrorCode.PersistentStreamPullingAgent_16, "Got back {0} Subscribers for stream {1}.", subscriptions.Value.Count, streamData.StreamId);

                var addSubscriptionTasks = new List<Task>(subscriptions.Value.Count);
                foreach (StreamSubscription<Guid> subscription in subscriptions.Value)
                {
                    IStreamConsumerExtension extension = subscription.Consumer.AsReference<IStreamConsumerExtension>();
                    addSubscriptionTasks.Add(AddSubscriber(streamData, subscription.SubscriptionId, streamData.StreamId, extension, streamStartToken, nowUtc));
                }
                await Task.WhenAll(addSubscriptionTasks);
            }
            catch (Exception exc)
            {
                // RegisterAsStreamProducer is fired with .Ignore so we should log if anything goes wrong, because there is no one to catch the exception
                logger.Error(ErrorCode.PersistentStreamPullingAgent_17, "Ignored RegisterAsStreamProducer Error", exc);
                throw;
            }
        }
    }
}
