using System;
using System.Threading.Tasks;
using BenchmarkGrainInterfaces.RecoverableStream;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Core;
using Orleans.Streams;

namespace BenchmarkGrains.RecoverableStreams
{
    [ImplicitStreamSubscription("test")]
    [ImplicitStreamSubscription("other")]
    public class RecoverableStreamGrain : Grain, IRecoverableStreamGrain
    {
        private readonly IRecoverableStream<State> recoverableStream;
        private readonly ILogger logger;

        public class State {};

        public RecoverableStreamGrain(
            [RecoverableStream("recoverable")]
            IRecoverableStream<State> recoverableStream,
            ILogger<RecoverableStreamGrain> logger)
        {
            this.recoverableStream = recoverableStream;
            this.logger = logger;
            recoverableStream.Attach(new MyProcessor(recoverableStream.StreamId, this.logger), new MyStorage(this.logger));
        }

        public override Task OnActivateAsync()
        {
            Guid key = this.GetPrimaryKey(out string keyExtension);
            this.logger.LogInformation("Activating {Grain}", key.ToString() + keyExtension);
            return base.OnActivateAsync();
        }

        private class MyProcessor : IRecoverableStreamProcessor<int, State>
        {
            private readonly IStreamIdentity streamId;
            private readonly ILogger logger;

            public MyProcessor(IStreamIdentity streamId, ILogger logger)
            {
                this.streamId = streamId;
                this.logger = logger;

            }

            public Task OnIdle(State state)
            {
                throw new NotImplementedException();
            }

            public Task<bool> ProcessEvent(int evt, StreamSequenceToken token, State state)
            {
                this.logger.LogInformation("Processing event {Event} for stream {Stream} at {Token}",
                    evt, this.streamId.Guid.ToString() + this.streamId.Namespace, token);
                return Task.FromResult(true);
            }

            public bool ShouldRetryRecovery(State state, int attemtps, Exception lastException, out TimeSpan retryInterval)
            {
                throw new NotImplementedException();
            }
        }

        private class MyStorage : IStorage<RecoverableStreamState<State>>
        {
            private readonly ILogger logger;

            public MyStorage(ILogger logger)
            {
                this.logger = logger;
            }

            public RecoverableStreamState<State> State { get; set; } = new RecoverableStreamState<State> { State = new State() };

            public string Etag => "blarg";

            public Task ClearStateAsync()
            {
                this.State = new RecoverableStreamState<State> { State = new State() };
                return Task.CompletedTask;
            }

            public Task ReadStateAsync()
            {
                return Task.CompletedTask;
            }

            public Task WriteStateAsync()
            {
                this.logger.LogInformation("Saving state for stream {Stream} at {Token}",
                    this.State.StreamId.Guid.ToString() + this.State.StreamId.Namespace, this.State.LastProcessedToken ?? this.State.StartToken);
                return Task.CompletedTask;
            }
        }
    }
}
