
using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public class IRecoverableStreamState<TState>
    {
        string ETag { get;}
        TState State { get;  }
        IStreamIdentity StreamId { get; }
        StreamSequenceToken StartToken { get; }
        StreamSequenceToken SequenceToken { get; }
        bool Idle { get; }
    }

    public interface IRecoverableStreamProcessor<TEvent, TState>
    {
        Task<bool> ProcessEvent(TEvent evt, StreamSequenceToken token, TState state);
        Task OnIdle(IRecoverableStreamState<TState> recoverableStreamState);
        bool ShouldRetryRecovery(int attemtps, Exception lastException, out TimeSpan retryInterval);

    }

    public interface IRecoverableStreamStorage<TState>
    {
        Task<string> Store(IRecoverableStreamState<TState> recoverableStreamState);
        Task<IRecoverableStreamState<TState>> Load();
    }

    public interface IRecoverableStream<TState>
    {
        IStreamIdentity StreamId { get; }
        TState State { get; }

        void Attach<TEvent>(
            IRecoverableStreamProcessor<TEvent, TState> streamingApp,
            IRecoverableStreamStorage<TState> storage);
    }
}
