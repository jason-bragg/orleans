
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
        where TState : new()
    {
        TState State { get; set; }

        string ETag { get; }

        Task ReadStateAsync();

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

        public Task ReadStateAsync()
        {
            return this.simpleStorage.ReadStateAsync();
        }

        public Task<AdvancedStorageWriteResultCode> WriteStateAsync()
        {
            this.simpleStorage.WriteStateAsync();

            return Task.FromResult(AdvancedStorageWriteResultCode.Success);
        } 
    }

    public interface ISubscriptionHandler
    {
        Task Subscribe(StreamSequenceToken token);
    }

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

    public interface ICheckpointPolicy
    {

    }

    public interface IStoragePolicy
    {
        bool ShouldBackoffOnWriteWithAmbiguousResult { get; }

        bool ShouldReloadOnWriteWithAmbiguousResult { get; } // Need metrics to decide on this. Probably tech specific. TODO: Maybe this should be a threshold

        TimeSpan GetBackoff(int attempts);
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
        private IAdvancedStorage<RecoverableStreamState<TState>> storage;
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

            await this.storage.ReadStateAsync();
            this.streamingStateETag = this.storage.ETag;
            this.streamingState = this.storage.State;
            if (this.streamingState == null)
            {
                this.streamingState = new RecoverableStreamState<TState>
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

        private async Task<bool> Checkpoint()
        {

        }

        // Return true if we FF'd. 
        private async Task<bool> Save()
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
                bool shouldBackoff = false;
                bool shouldReload;

                switch (writeResult)
                {
                    case AdvancedStorageWriteResultCode.Success:
                        return false; // We didn't need to FF
                    case AdvancedStorageWriteResultCode.Ambiguous:
                        shouldBackoff = this.storagePolicy.ShouldBackoffOnWriteWithAmbiguousResult;
                        shouldReload = this.storagePolicy.ShouldReloadOnWriteWithAmbiguousResult;
                        break;
                    case AdvancedStorageWriteResultCode.Conflict:
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
                    var backoff = this.storagePolicy.GetBackoff(attempts);

                    if (backoff > TimeSpan.Zero)
                    {
                        await Task.Delay(backoff);
                    }
                }

                if (shouldReload)
                {
                    await this.storage.ReadStateAsync();

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

                                // Fast forward
                                await this.Subscribe(storedStreamingState.GetToken());

                                // TODO: Notify of FF

                                return true; // We needed to FF
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

        private Task Recover()
        {
            throw new NotImplementedException(nameof(Recover));
        }
    }
}
