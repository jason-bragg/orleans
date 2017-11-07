using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Pipes.Abstractions
{
    #region producer

    public interface IOutPushPipeSender<TData>
    {
        Task Send(TData[] data);
        Task Close(Exception ex = null);
    }

    public interface IOutPushPipe
    {
        IOutPushPipeSender<TData> CreateSender<TKey, TData>(TKey key, GrainReference grainRef);
    }

    #endregion producer
    #region consumer

    public interface IPushHandler<TData>
    {
        Task OnData(TData[] data);
    }

    public interface IInPushPipeListener
    {
        IDisposable Listen<TKey, TData>(TKey key, Func<TKey, IPushHandler<TData>> handlerFactory, Action<TKey, IDisposable> registerAction);
        IDisposable Listen<TKey, TData>(TKey key, IPushHandler<TData> handler);
    }

    #endregion consumer
}
