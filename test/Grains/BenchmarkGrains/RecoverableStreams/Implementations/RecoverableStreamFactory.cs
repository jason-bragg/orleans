using Orleans.Runtime;

namespace Orleans.Streams
{
    public class RecoverableStreamFactory : IRecoverableStreamFactory
    {
        public IRecoverableStream<TState> Create<TState>(IGrainActivationContext context, IRecoverableStreamConfiguration config) where TState : new()
        {
            IStreamProvider provider = context.ActivationServices.GetServiceByName<IStreamProvider>(config.StreamProviderName);
            return new RecoverableStream<TState>(provider);
        }
    }
}
