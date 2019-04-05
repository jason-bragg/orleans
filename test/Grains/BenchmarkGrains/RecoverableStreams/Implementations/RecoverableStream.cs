
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.CodeGeneration;
using Orleans.Core;
using Orleans.Runtime;

[assembly: GenerateSerializer(typeof(Orleans.Streams.RecoverableStreamState<>))]

namespace Orleans.Streams
{
    public enum TimerType
    {
        ActivityTracker,
        Recovery,
    }

    public class TimerState
    {
        public IStreamIdentity StreamId { get; set; }

        public TimerType TimerType { get; set; }
    }

    public interface INonReentrantTimerCallbackGrainExtension : IGrainExtension
    {
        Task OnNonReentrantTimer(object state);
    }

    public enum AdvancedStorageReadResultCode
    {
        Success,

        Throttled,

        GeneralFailure,
    }

    public enum AdvancedStorageWriteResultCode
    {
        /// <summary>
        /// The write was committed successfully.
        /// </summary>
        Success,

        /// <summary>
        /// The write completed in an ambiguous manner in which the write could have committed
        /// successfully or unsuccessfully.
        /// </summary>
        /// <remarks>
        /// This could be any number of cases including but not limited to:
        ///   - Timeout
        ///   - Task Cancelled
        /// </remarks>
        Ambiguous,

        /// <summary>
        /// The write was rejected and not committed due to a conflict.
        /// </summary>
        /// <remarks>
        /// This could be any number of cases including but not limited to:
        ///   - Entity already exists when trying to insert
        ///   - ETag mismatch error
        /// </remarks>
        Conflict,

        /// <summary>
        /// The write was rejected and not committed due to being throttled for traffic.
        /// </summary>
        /// <remarks>
        /// This could be any number of cases including but not limited to:
        ///   - Storage throttling (HTTP 429)
        ///   - Storage HTTP 5xx
        /// </remarks>
        Throttled,

        /// <summary>
        /// The write was not committed due to an unclassified general error.
        /// </summary>
        GeneralFailure,
    }

    public interface IAdvancedStorage<TState>
    {
        TState State { get; set; }

        string ETag { get; }

        Task<AdvancedStorageReadResultCode> ReadStateAsync();

        Task<AdvancedStorageWriteResultCode> WriteStateAsync();
    }

    public class AdvancedStorageSimpleStorageAdapter<TState> : IAdvancedStorage<TState>
        where TState : new()
    {
        private readonly IStorage<TState> simpleStorage;

        public AdvancedStorageSimpleStorageAdapter(IStorage<TState> simpleStorage)
        {
            if (simpleStorage == null)
            {
                throw new ArgumentNullException(nameof(simpleStorage));
            }

            this.simpleStorage = simpleStorage;
        }

        public TState State
        {
            get => this.simpleStorage.State;
            set => this.simpleStorage.State = value;
        }

        public string ETag => this.simpleStorage.Etag;

        public async Task<AdvancedStorageReadResultCode> ReadStateAsync()
        {
            await this.simpleStorage.ReadStateAsync();

            return AdvancedStorageReadResultCode.Success;
        }

        public async Task<AdvancedStorageWriteResultCode> WriteStateAsync()
        {
            await this.simpleStorage.WriteStateAsync();

            return AdvancedStorageWriteResultCode.Success;
        } 
    }

    public interface ISubscriptionHandler
    {
        Task Subscribe(StreamSequenceToken token);
    }

    // TODO: Hm maybe I should be having delegates/events that can be listened to.
    public interface IStreamMonitor
    {
        void NotifyActive(StreamSequenceToken token);

        void NotifyOnNextSuccess(StreamSequenceToken token);

        void NotifyOnNextFailure(StreamSequenceToken token, Exception exception);

        void NotifyCheckpoint(StreamSequenceToken token);

        void NotifySave(StreamSequenceToken token);

        void NotifyInactive(StreamSequenceToken token);

        void NotifyRecovery(StreamSequenceToken token);

        void NotifyOnError(Exception exception);
    }

    public class StreamMonitorBase : IStreamMonitor
    {
        public virtual void NotifyActive(StreamSequenceToken token)
        {
        }

        public virtual void NotifyOnNextSuccess(StreamSequenceToken token)
        {
        }

        public virtual void NotifyOnNextFailure(StreamSequenceToken token, Exception exception)
        {
        }

        public virtual void NotifyCheckpoint(StreamSequenceToken token)
        {
        }

        public virtual void NotifySave(StreamSequenceToken token)
        {
        }

        public virtual void NotifyInactive(StreamSequenceToken token)
        {
        }

        public virtual void NotifyRecovery(StreamSequenceToken token)
        {
        }

        public virtual void NotifyOnError(Exception exception)
        {
        }
    }

    public interface IRecoveryBackoffPolicy : IStreamMonitor
    {
        TimeSpan GetBackoff();
    }

    public class BasicRecoveryBackoffPolicy : StreamMonitorBase, IRecoveryBackoffPolicy
    {
        private StreamSequenceToken recoverySequenceToken;
        private int recoveryCount = 0;
        
        public override void NotifyRecovery(StreamSequenceToken token)
        {
            if (this.recoverySequenceToken == null || !token.Equals(this.recoverySequenceToken))
            {
                this.recoverySequenceToken = token;
                this.recoveryCount = 0;
            }

            ++this.recoveryCount;
        }

        public TimeSpan GetBackoff()
        {
            return TimeSpan.FromSeconds(5 * (this.recoveryCount- 1));
        }

    }

    public interface IPoisonEventHandler : IStreamMonitor
    {
        bool ClassifyEventPoisonStatus(StreamSequenceToken token);
    }

    public interface IActivityTracker : IStreamMonitor
    {

    }

    public interface IAdvancedTimerManager
    {
        IDisposable RegisterReentrantTimer(string timerName, Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period);

        IDisposable RegisterNonReentrantTimer(string timerName, Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period);
    }

    // TODO: Potential conflict between grain timeout and timer frequencies. Possible to warn or even delay?
    public class RecoverableStream<TState, TEvent> : IRecoverableStream<TState, TEvent>, ILifecycleParticipant<IGrainLifecycle>, INonReentrantTimerCallbackGrainExtension where TState : new()
    {
        private readonly IStreamProvider streamProvider;
        private readonly ILogger logger;
        private readonly IGrainActivationContext context;
        private readonly IGrainRuntime runtime;
        private IRecoverableStreamProcessor<TState, TEvent> processor;
        private IRecoverableStreamStorage<TState> storage;
        //private string streamingStateETag; // TODO: Not sure I need this.
        private RecoverableStreamState<TState> streamingState => this.storage.State;

        private IStoragePolicy storagePolicy;

        public RecoverableStream(IStreamProvider streamProvider, IStreamIdentity streamId, IGrainActivationContext context, ILogger<RecoverableStream<TState, TEvent>> logger, IGrainRuntime runtime)
        {
            if (!streamProvider.IsRewindable)
            {
                throw new ArgumentException("String Provider must be Rewindable", nameof(streamProvider));
            }

            this.streamProvider = streamProvider;
            this.StreamId = streamId;
            this.context = context;
            this.logger = logger;
            this.runtime = runtime;
        }

        public IStreamIdentity StreamId { get; }

        public TState State
        {
            get
            {
                if (this.streamingState == null)
                {
                    return default;
                }

                return this.streamingState.ApplicationState;
            }
        }

        public void Attach(IRecoverableStreamProcessor<TState, TEvent> processor, IAdvancedStorage<RecoverableStreamState<TState>> storage)
        {
            this.processor = processor;
            this.storage = storage;
        }

        public void Participate(IGrainLifecycle lifecycle)
        {
            lifecycle.Subscribe(this.GetType().FullName, GrainLifecycleStage.SetupState+1, OnSetupState, OnCleanupState);
        }

        // TODO: What happens if I throw here? Do I get retried? Should I handle my own retries here? We might not be an implicit stream.
        private async Task OnSetupState(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await this.storage.Load();
            if (this.storage.State == null) // TODO: Will this actually come back null? What's the expectation from Orleans?
            {
                this.storage.State = new RecoverableStreamState<TState>
                {
                    StreamId = this.StreamId,
                    ApplicationState = new TState()
                };
            }

            if (!this.streamingState.IsIdle)
            {
                // TODO: Get current token
                await this.Subscribe(this.streamingState.CurrentToken ?? this.streamingState.StartToken);
            }
            else
            {
                await this.Subscribe(null);
            }

            // TODO: Register timers
        }

        private Task OnCleanupState(CancellationToken cancellationToken)
        {
            // TODO: Best effort save and log if it didn't work. Don't set idle if it's not time.
            return Task.CompletedTask;
        }

        private async Task Subscribe(StreamSequenceToken sequenceToken)
        {
            var stream = this.streamProvider.GetStream<TEvent>(this.StreamId.Guid, this.StreamId.Namespace);

            var handles = await stream.GetAllSubscriptionHandles();

            try
            {
                if (handles.Count == 0)
                {
                    await stream.SubscribeAsync(this.OnNext, this.OnError, sequenceToken);
                }
                else
                {
                    if (handles.Count > 1)
                    {
                        // TODO: Warn
                    }

                    await handles.First().ResumeAsync(this.OnNext, this.OnError, sequenceToken);
                }
            }
            catch (Exception exception)
            {
                // TODO: Warn
                Console.WriteLine(exception);
                throw;
            }
        }

        private async Task OnNext(TEvent @event, StreamSequenceToken token)
        {
            if (this.streamingState.IsDuplicateEvent(token))
            {
                return;
            }

            if (this.streamingState.StartToken == null || this.streamingState.IsIdle)
            {
                this.streamingState.ResetTokens();
                this.streamingState.SetStartToken(token);

                this.streamingState.IsIdle = false;

                // TODO: Save and handle storage conflict
            }

            this.streamingState.SetCurrentToken(token);

            bool processorRequestsSave;
            try
            {
                processorRequestsSave = await this.processor.OnEvent(@event, token, this.storage.State.ApplicationState);;
            }
            catch (Exception exception)
            {
                // TODO: Enter recovery
            }
            
            if (processorRequestsSave)
            {
                // TODO: Save and handle storage conflict
            }
        }

        private Task OnError(Exception exception)
        {
            return Task.CompletedTask;
        }

        private Task Recover()
        {
            throw new NotImplementedException(nameof(Recover));
        }
    }

    internal interface IRecoverableStreamStorage<TState>
    {
        RecoverableStreamState<TState> State { get; set; }

        Task Load();

        Task<bool> Save();

        Task<bool> Checkpoint();
    }

    public interface IRecoverableStreamStoragePolicy
    {
        TimeSpan GetReadBackoff(AdvancedStorageReadResultCode resultCode, int attempts);

        bool ShouldBackoffOnWriteWithAmbiguousResult { get; }

        bool ShouldReloadOnWriteWithAmbiguousResult { get; } // Need metrics to decide on this. Probably tech specific. TODO: Maybe this should be a threshold

        TimeSpan GetWriteBackoff(AdvancedStorageWriteResultCode resultCode, int attempts);
    }

    internal class RecoverableStreamStorage<TState> : IRecoverableStreamStorage<TState>
    {
        private IAdvancedStorage<RecoverableStreamState<TState>> storage;
        private IRecoverableStreamStoragePolicy policy;

        public RecoverableStreamStorage(IAdvancedStorage<RecoverableStreamState<TState>> storage, IRecoverableStreamStoragePolicy policy)
        {
            if (storage == null) { throw new ArgumentNullException(nameof(storage)); }
            if (policy == null) { throw new ArgumentNullException(nameof(policy)); }

            this.storage = storage;
            this.policy = policy;
        }

        public RecoverableStreamState<TState> State { get; set; }

        public async Task Load()
        {
            int attempts = 0;

            // This loop will exit when the state is loaded
            // TODO: Should there be a max attempts here?
            while (true)
            {
                AdvancedStorageReadResultCode readResult;

                try
                {
                    ++attempts;
                    readResult = await this.storage.ReadStateAsync();
                }
                catch (Exception exception)
                {
                    readResult = AdvancedStorageReadResultCode.GeneralFailure;

                    // TODO: Log
                    Console.WriteLine(exception);
                }

                bool shouldBackoff;

                switch (readResult)
                {
                    case AdvancedStorageReadResultCode.Success:
                        return;
                    case AdvancedStorageReadResultCode.Throttled:
                        shouldBackoff = true;
                        break;
                    case AdvancedStorageReadResultCode.GeneralFailure:
                        shouldBackoff = false;
                        break;
                    default:
                        throw new NotSupportedException(FormattableString.Invariant($"Unknown {nameof(AdvancedStorageReadResultCode)} '{readResult}' returned by storage"));
                }

                if (shouldBackoff)
                {
                    var backoff = this.policy.GetReadBackoff(readResult, attempts);

                    if (backoff > TimeSpan.Zero)
                    {
                        await Task.Delay(backoff);
                    }
                }
            }
        }

        // TODO: Not a fan of this code or Load even. It's hard to see the exit conditions. This will make logging difficult.
        // Return true if we FF'd. 
        public async Task<bool> Save()
        {
            int attempts = 0;

            // This loop will exit when either the state is saved or the state is synchronized with what's in storage
            // TODO: Should there be a max attempts here?
            while (true)
            {
                var currentStreamingState = this.storage.State;
                var currentStreamingStateETag = this.storage.ETag;

                AdvancedStorageWriteResultCode writeResult;

                try
                {
                    ++attempts;
                    writeResult = await this.storage.WriteStateAsync();
                }
                catch (Exception exception)
                {
                    writeResult = AdvancedStorageWriteResultCode.GeneralFailure;

                    // TODO: Log
                    Console.WriteLine(exception);
                }

                // TODO: For safety we should limit the number of "optimized / skipped reloads we do"
                bool shouldBackoff;
                bool shouldReload;

                switch (writeResult)
                {
                    case AdvancedStorageWriteResultCode.Success:
                        return false; // We didn't need to FF
                    case AdvancedStorageWriteResultCode.Ambiguous:
                        shouldBackoff = this.policy.ShouldBackoffOnWriteWithAmbiguousResult;
                        shouldReload = this.policy.ShouldReloadOnWriteWithAmbiguousResult;
                        break;
                    case AdvancedStorageWriteResultCode.Conflict:
                        shouldBackoff = false;
                        shouldReload = true;
                        break;
                    case AdvancedStorageWriteResultCode.Throttled:
                        shouldBackoff = true;
                        shouldReload = false;
                        break;
                    case AdvancedStorageWriteResultCode.GeneralFailure:
                        // It's possible the storage implementation doesn't distinguish between failures. This means that even "Conflict" would look like a "General Failure". We must reload in this case - otherwise we'd never resolve the conflict. Storage implementations have an optimization opportunity to skip a potentially unnecessary load by signaling either "Ambiguous" or "Throttled".
                        shouldBackoff = true;
                        shouldReload = true;
                        break;
                    default:
                        // TODO: Log
                        throw new NotSupportedException(FormattableString.Invariant($"Unknown {nameof(AdvancedStorageWriteResultCode)} '{writeResult}' returned by storage"));
                }

                if (shouldBackoff)
                {
                    var backoff = this.policy.GetWriteBackoff(writeResult, attempts);

                    if (backoff > TimeSpan.Zero)
                    {
                        await Task.Delay(backoff);
                    }
                }

                if (shouldReload)
                {
                    // TODO: Maybe attempts should be carried in to here. Unfortunately right now "attempts" includes transient failures and conflicts and we probably only want to carry transient failures in.
                    await this.Load();

                    var storedStreamingState = this.storage.State;
                    var storedStreamingStateETag = this.storage.ETag;

                    if (currentStreamingStateETag != storedStreamingStateETag)
                    {
                        // The ETags don't match so something was actually written to storage. Let's comapre ot that.
                        var comparison = currentStreamingState.EasyCompareTo(storedStreamingState);

                        // TODO: Handle Idle differences

                        switch (comparison)
                        {
                            case EasyCompareToResult.Before:
                                {
                                    // We're behind. Use what's currently in storage.

                                    return true; // We need to FF
                                }
                            case EasyCompareToResult.Equal:
                                {
                                    // We're equal. We could take either state but we'll keep ours because maybe the GC implications are better.
                                    this.storage.State = currentStreamingState;

                                    return false; // We didn't need to FF.
                                }
                            case EasyCompareToResult.After:
                                {
                                    // We're ahead. Overwrite storage.
                                    this.storage.State = currentStreamingState;

                                    break;
                                }
                            default:
                                {
                                    throw new NotSupportedException($"Unknown {nameof(EasyCompareToResult)} '{comparison}'");
                                }
                        }
                    }
                }

                // If we got this far it means we need to save
            }
        }

        public Task<bool> Checkpoint()
        {
            // TODO: This can probably share code with Save. But we need to consider the checkpoint policy. If the checkpoint fails transiently, we should evaluate the retry policy and see how much we care. Of course if it's conflict we probably want to resolve it. If it's general failure we probably need to reload because we won't know if the storage impl bothers to return special codes or if we're in a fallback.
            throw new NotImplementedException();
        }
    }
}
