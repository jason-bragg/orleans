using System;
using System.Threading.Tasks;
using BenchmarkGrainInterfaces.RecoverableStream;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.Runtime;
using Orleans.Streams;

[assembly: GenerateSerializer(typeof(Orleans.Streams.RecoverableStreamState<BenchmarkGrains.RecoverableStreams.RecoverableStreamGrain.State>))]

namespace BenchmarkGrains.RecoverableStreams
{
    [ImplicitStreamSubscription("test")]
    [ImplicitStreamSubscription("other")]
    public class RecoverableStreamGrain : Grain, IRecoverableStreamGrain
    {
        private readonly IRecoverableStream<State> recoverableStream;
        private readonly ILogger logger;

        public class State
        {
            public int Count;
        };

        public RecoverableStreamGrain(
            [RecoverableStream("recoverable")]
            IRecoverableStream<State> recoverableStream,
            [PersistentState("state")]
            IPersistentState<RecoverableStreamState<State>> streamState,
            ILogger<RecoverableStreamGrain> logger)
        {
            this.recoverableStream = recoverableStream;
            this.logger = logger;
            recoverableStream.Attach(new MyProcessor(recoverableStream.StreamId, this.logger), streamState);
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
                state.Count++;
                this.logger.LogInformation("Event Count is {Count} for stream {Stream} at {Token}",
                    state.Count, this.streamId.Guid.ToString() + this.streamId.Namespace, token);
                return Task.FromResult(true);
            }

            public bool ShouldRetryRecovery(State state, int attemtps, Exception lastException, out TimeSpan retryInterval)
            {
                throw new NotImplementedException();
            }
        }
    }
}
