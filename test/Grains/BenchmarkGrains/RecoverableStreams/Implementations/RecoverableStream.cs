
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.CodeGeneration;
using Orleans.Runtime;
using Orleans.Streams;

[assembly: GenerateSerializer(typeof(RecoverableStreamState<>))]

namespace Orleans.Streams
{
    public enum TimerType
    {
        ActivityTracker,
        Recovery
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
        private int recoveryCount;
        
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

    public delegate Task AdvancedTimerCallback(IDisposable timer, object state);

    public interface IAdvancedTimerManager
    {
        IDisposable RegisterReentrantTimer(AdvancedTimerCallback callback, object state, TimeSpan dueTime, TimeSpan period);

        IDisposable RegisterNonReentrantTimer(AdvancedTimerCallback callback, object state, TimeSpan dueTime, TimeSpan period);
    }

    // TODO: Mega TODOs
    //   - Recovery backoff
    //   - Idle detection via timers and lifecycle control
    //   - Graceful limping (cut until we need it but then it'll probably be a "context" that we tack on to OnNext())
    // TODO: Potential conflict between grain timeout and timer frequencies. Possible to warn or even delay?
    public class RecoverableStream<TState, TEvent> : IRecoverableStream<TState, TEvent>, ILifecycleParticipant<IGrainLifecycle>, INonReentrantTimerCallbackGrainExtension where TState : new()
    {
        private readonly IStreamProvider streamProvider;
        private readonly ILogger logger;
        private readonly IGrainActivationContext context;
        private readonly IGrainRuntime runtime;
        private readonly IAdvancedTimerManager timerManager; // TODO: Instantiate me using all kinds of crazy Orleans stuff

        private IRecoverableStreamProcessor<TState, TEvent> processor;
        private IRecoverableStreamStorage<TState> storage;

        public RecoverableStream(IStreamProvider streamProvider, IStreamIdentity streamId, IGrainActivationContext context, ILogger<RecoverableStream<TState, TEvent>> logger, IGrainRuntime runtime)
        {
            if (streamProvider == null) { throw new ArgumentNullException(nameof(streamProvider)); }
            if (!streamProvider.IsRewindable) { throw new ArgumentException("Stream Provider must be Rewindable", nameof(streamProvider)); }

            if (streamId == null) { throw new ArgumentNullException(nameof(streamId)); }

            if (context == null) { throw new ArgumentNullException(nameof(context)); }

            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            if (runtime == null) { throw new ArgumentNullException(nameof(runtime)); }

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
                if (this.storage.State == null)
                {
                    return default;
                }

                return this.storage.State.ApplicationState;
            }
        }

        // TODO: Handle if events or timer ticks start coming in before we're attached. Or maybe we should make that not be possible?
        public void Attach(
            IRecoverableStreamProcessor<TState, TEvent> processor,
            IAdvancedStorage<RecoverableStreamState<TState>> storage,
            IRecoverableStreamStoragePolicy storagePolicy)
        {
            if (processor == null) { throw new ArgumentNullException(nameof(processor)); }
            if (storage == null) { throw new ArgumentNullException(nameof(storage)); }
            if (storagePolicy == null) { throw new ArgumentNullException(nameof(storagePolicy)); }

            if (this.processor == null)
            {
                throw new InvalidOperationException("Stream already has Processor attached");
            }

            this.processor = processor;
            this.storage = new RecoverableStreamStorage<TState>(storage, storagePolicy);
        }

        public void Participate(IGrainLifecycle lifecycle)
        {
            lifecycle.Subscribe(this.GetType().FullName, GrainLifecycleStage.SetupState + 1, OnSetupState, OnCleanupState);
        }

        // TODO: What happens if I throw here? Do I get retried? Should I handle my own retries here? We might not be an implicit stream.
        // TODO: Handle what to do if we're not attached. Throw? Put ourselves into a no-op state?
        private async Task OnSetupState(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await this.storage.Load();
            if (this.storage.State == null) // TODO: Will this actually come back null? What's the expectation from Orleans IStorage?
            {
                this.storage.State = new RecoverableStreamState<TState>
                {
                    StreamId = this.StreamId,
                    ApplicationState = new TState()
                };
            }

            if (!this.storage.State.IsIdle)
            {
                // TODO: Get current token
                await this.Subscribe(this.storage.State.GetToken());
            }
            else
            {
                await this.Subscribe(null);
            }

            // TODO: Apply jitter to timer registration
            this.timerManager.RegisterReentrantTimer(this.OnCheckpointTimer, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

            // TODO: Recovery?
            await this.processor.OnSetup(this.storage.State.ApplicationState, this.storage.State.GetToken());
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
                // TODO: Warn. 
                Console.WriteLine(exception);
                throw; // TODO: Not sure what else we can do here. We're not doing explicit pub sub but we can't guarantee we're on the same silo so this could still fail.
            }
        }

        private Task OnCheckpointTimer(IDisposable timer, object state)
        {
            return this.CheckpointIfOverdue();
        }

        private async Task OnNext(TEvent @event, StreamSequenceToken token)
        {
            try
            {
                if (this.storage.State.IsDuplicateEvent(token))
                {
                    return;
                }

                if (this.storage.State.StartToken == null || this.storage.State.IsIdle)
                {
                    this.storage.State.ResetTokens();
                    this.storage.State.SetStartToken(token);

                    this.storage.State.IsIdle = false;

                    var saveFastForwarded = await this.Save();

                    if (saveFastForwarded)
                    {
                        // We fast-forwarded so it's possible that the current event is now considered a duplicate
                        if (this.storage.State.IsDuplicateEvent(token))
                        {
                            return;
                        }
                    }

                    await this.processor.OnActiveStream(this.storage.State.ApplicationState, token);
                }

                this.storage.State.SetCurrentToken(token);

                // TODO: Handle Poison Events here. I'm actually thinking this could be a sub-processor!
                var processorRequestsSave = await this.processor.OnEvent(this.storage.State.ApplicationState, token, @event);

                // TODO: Checkpoint

                if (processorRequestsSave)
                {
                    await this.storage.Save();
                }
            }
            catch (Exception exception)
            {
                // TODO: Log
                Console.WriteLine(exception);

                await this.storage.Load();

                // TODO: Should we have OnRecovery recovery?
                await this.processor.OnRecovery(this.storage.State.ApplicationState, token);

                await this.Subscribe(this.storage.State.GetToken());

                // TODO: Hm. If there are other batches queued for delivery outside the grain, will those get delivered first? Are the streaming extensions smarter than that?
                // TODO: Do we actually need to throw here? We requested a rewind, shouldn't that be sufficient?
            }
        }

        private Task<bool> CheckpointIfOverdue()
        {
            return this.Persist(isCheckpoint: true);
        }

        private Task<bool> Save()
        {
            return this.Persist(isCheckpoint: false);
        }

        // TODO: Maybe this should be pushed down to the storage interface, but it's kinda nice how that has limited dependencies right now
        // Did we FF?
        private async Task<bool> Persist(bool isCheckpoint)
        {
            bool persisted, fastForwardRequested;
            if (isCheckpoint)
            {
                (persisted, fastForwardRequested) = await this.storage.CheckpointIfOverdue();
            }
            else
            {
                persisted = true;
                fastForwardRequested = await this.storage.Save();
            }
            
            if (fastForwardRequested)
            {
                await this.Subscribe(this.storage.State.GetToken());
            }

            if (persisted)
            {
                await this.processor.OnPersisted(this.storage.State.ApplicationState, this.storage.State.GetToken(),
                    fastForwardRequested); // TODO: Should token be passed in? Save is sometimes call before current token is set
            }

            return fastForwardRequested;
        }

        private Task OnError(Exception exception)
        {
            // TODO: Log

            if (this.processor == null)
            {
                return Task.CompletedTask;
            }

            // TODO: Save if requested
            // TODO: Error handling?
            // TODO: Null handling?
            return this.processor.OnError(this.storage.State.ApplicationState, this.storage.State.GetToken(), exception, new StreamingFailureErrorArgs());
        }
    }
}
