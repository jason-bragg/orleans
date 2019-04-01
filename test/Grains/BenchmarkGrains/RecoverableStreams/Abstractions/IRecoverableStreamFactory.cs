
using Orleans.Runtime;

namespace Orleans.Streams
{
    public interface IRecoverableStreamFactory
    {
        IRecoverableStream<TState> Create<TState>(IGrainActivationContext context, IRecoverableStreamConfiguration config) where TState : new();
    }
}
