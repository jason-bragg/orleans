
using System;
using System.Threading.Tasks;
using Orleans.Core;

namespace Orleans.Streams
{
    public class RecoverableStreamState<TState>
    {
        public IStreamIdentity StreamId { get; set; }
        public TState State { get; set; }
        public StreamSequenceToken StartToken { get; set; }
        public StreamSequenceToken LastProcessedToken { get; set; }
        public bool Idle { get; set; }
    }

    public interface IRecoverableStreamProcessor<TEvent, TState>
    {
        Task<bool> ProcessEvent(TEvent evt, StreamSequenceToken token, TState state);
        Task OnIdle(TState state);
        bool ShouldRetryRecovery(TState state, int attemtps, Exception lastException, out TimeSpan retryInterval);

    }

    public interface IRecoverableStream<TState>
    {
        IStreamIdentity StreamId { get; }
        TState State { get; }

        void Attach<TEvent>(
            IRecoverableStreamProcessor<TEvent, TState> streamingApp,
            IStorage<RecoverableStreamState<TState>> storage);
    }
}
