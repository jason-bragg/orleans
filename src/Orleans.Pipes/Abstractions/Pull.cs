using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Pipes.Abstractions
{
    public interface IPullPipeResult<TToken, TData>
    {
        TToken ContinuationToken { get; }
        Task<TData[]> Results { get; }
    }

    public interface IPullPipeIn<TKey, TToken, TData>
    {
        TKey Id { get; }
        Task<IPullPipeResult<TToken,TData>> Pull(TToken ack);
    }

    public interface IPullPipeGrainConnector
    {
        IPullPipeIn<TKey, TToken, TData> Create<TKey, TToken, TData>(TKey key, GrainReference grainRef, TToken token);
    }

    public interface IPullPipeOut<TKey, TToken, TData>
    {
        TKey Id { get; }
        Task<TData[]> OnPull(TToken ack);
    }

    public interface IPullPipeListener
    {
        IDisposable Listen<TKey, TToken, TData>(Func<TKey, TToken, IPullPipeOut<TKey, TToken, TData>> outFactory, Action<TKey, IDisposable> outRegistery);
        IDisposable Listen<TKey, TToken, TData>(TKey key, IPullPipeOut<TKey, TToken, TData> pullPipeOut);
    }
}
