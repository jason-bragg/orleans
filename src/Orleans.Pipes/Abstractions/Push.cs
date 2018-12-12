using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Pipes.Abstractions
{
    public interface IPushPipeOut<TKey, TData>
    {
        TKey Id { get; }
        Task Send(TData[] data);
        Task Close(Exception ex = null);
    }

    public interface IPushPipeGrainConnector
    {
        IPushPipeOut<TKey, TData> Create<TKey, TData>(TKey key, GrainReference grainRef);
    }

    public interface IPushPipeIn<TKey, TData>
    {
        TKey Id { get; }
        Task OnData(TData[] data);
    }

    public interface IPushPipeListener
    {
        IDisposable Listen<TKey, TData>(Func<TKey, IPushPipeIn<TKey, TData>> inFactory, Action<TKey, IDisposable> inRegistery);
        IDisposable Listen<TKey, TData>(TKey key, IPushPipeIn<TKey, TData> pushPipeIn);
    }
}
