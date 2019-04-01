using System;
using System.Threading.Tasks;
using BenchmarkGrainInterfaces.RecoverableStream;
using Microsoft.Extensions.Logging;
using Orleans;
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
            recoverableStream.Attach<int>(null, null);
        }

        public override Task OnActivateAsync()
        {
            Guid key = this.GetPrimaryKey(out string keyExtension);
            this.logger.LogInformation("Activating {Grain}", key.ToString() + keyExtension);
            return base.OnActivateAsync();
        }
    }
}
