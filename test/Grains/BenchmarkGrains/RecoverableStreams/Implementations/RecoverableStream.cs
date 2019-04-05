
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.CodeGeneration;
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
            if (this.storage.State == null) // TODO: Will this actually come back null? What's the expectation from Orleans IStorage?
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
}
