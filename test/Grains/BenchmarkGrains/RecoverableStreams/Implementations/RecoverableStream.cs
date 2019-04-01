
using System;
using System.Collections.Generic;
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

    public class RecoverableStream<TState> : IRecoverableStream<TState>, ILifecycleParticipant<IGrainLifecycle>
    {
        private readonly IStreamProvider streamProvider;
        private readonly ILogger logger;
        private IStreamProcessor processor;


        public RecoverableStream(IStreamProvider streamProvider, IStreamIdentity streamId, ILogger<RecoverableStream<TState>> logger)
        {
            this.streamProvider = streamProvider;
            this.StreamId = streamId;
            this.logger = logger;
        }

        public IStreamIdentity StreamId { get; }

        public TState State => this.processor.State;

        public void Attach<TEvent>(IRecoverableStreamProcessor<TEvent, TState> streamingApp, IStorage<RecoverableStreamState<TState>> storage)
        {
            this.processor = new StreamProcessor<TEvent>(streamProvider, StreamId, streamingApp, storage);
        }

        public void Participate(IGrainLifecycle lifecycle)
        {
            lifecycle.Subscribe(this.GetType().FullName, GrainLifecycleStage.SetupState+1, OnSetupState);
        }

        private async Task OnSetupState(CancellationToken ct)
        {
            if (ct.IsCancellationRequested)
                return;
            await this.processor.Load();
            await this.processor.Subscribe();
        }

        private interface IStreamProcessor
        {
            TState State { get; }
            Task Load();
            Task Subscribe();
        }

        private class StreamProcessor<TEvent> : IStreamProcessor
        {
            private readonly IStreamProvider streamProvider;
            private readonly IStreamIdentity streamId;
            private readonly IRecoverableStreamProcessor<TEvent, TState> streamingApp;
            private readonly IStorage<RecoverableStreamState<TState>> storage;

            private StreamSequenceToken RecoveryToken { get { return this.storage.State.LastProcessedToken ?? this.storage.State.StartToken; } }

            public TState State => this.storage.State.State;

            public StreamProcessor(IStreamProvider streamProvider, IStreamIdentity streamId, IRecoverableStreamProcessor<TEvent, TState> streamingApp, IStorage<RecoverableStreamState<TState>> storage)
            {
                this.streamProvider = streamProvider;
                this.streamId = streamId;
                this.streamingApp = streamingApp;
                this.storage = storage;
            }

            public async Task Load()
            {
                await this.storage.ReadStateAsync();
                if(this.storage.State.StreamId == null)
                {
                    this.storage.State.StreamId = streamId;
                    this.storage.State.State = Activator.CreateInstance<TState>();
                }
            }

            public async Task Subscribe()
            {
                IAsyncStream<TEvent> stream = this.streamProvider.GetStream<TEvent>(this.streamId.Guid, this.streamId.Namespace);
                IList<StreamSubscriptionHandle<TEvent>> handles = await stream.GetAllSubscriptionHandles();
                if(handles.Count == 0)
                {
                    await stream.SubscribeAsync(ProcessEvent, OnError, this.RecoveryToken);
                } else
                {
                    await handles.First().ResumeAsync(ProcessEvent, OnError, this.RecoveryToken);
                }
            }

            private async Task ProcessEvent(TEvent evt, StreamSequenceToken token)
            {
                if (IsDuplicate(token))
                    return;

                try
                {
                    if (TryUpdateStartToken(token))
                    {
                        await this.storage.WriteStateAsync();
                    };

                    bool store = await this.streamingApp.ProcessEvent(evt, token, this.storage.State.State);
                    this.storage.State.LastProcessedToken = token;
                    if (store)
                    {
                        await this.storage.WriteStateAsync();
                    }
                } catch
                {
                    await Recover();
                }
            }

            private Task OnError(Exception ex)
            {
                return Task.CompletedTask;
            }

            private bool IsDuplicate(StreamSequenceToken sequenceToken)
            {
                // This is the first event, so it can't be a duplicate
                if (this.storage.State.StartToken == null)
                    return false;

                // if we have processed events, compare with the sequence token of last event we processed.
                if (this.storage.State.LastProcessedToken != null)
                {
                    // if Last processed is not older than this sequence token, then this token is a duplicate
                    return !this.storage.State.LastProcessedToken.Older(sequenceToken);
                }

                // If all we have is the start token, then we've not processed the first event, so we should process any event at or after the start token.
                return this.storage.State.StartToken.Newer(sequenceToken);
            }

            private bool TryUpdateStartToken(StreamSequenceToken sequenceToken)
            {
                if (this.storage.State.StartToken == null)
                {
                    this.storage.State.StartToken = sequenceToken;
                    return true;
                }
                return false;
            }

            private Task Recover()
            {
                throw new NotImplementedException(nameof(Recover));
            }
        }
    }
}
