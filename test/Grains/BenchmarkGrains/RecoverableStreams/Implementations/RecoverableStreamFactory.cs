using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public class RecoverableStreamFactory : IRecoverableStreamFactory
    {
        public IRecoverableStream<TState> Create<TState>(IGrainActivationContext context, IRecoverableStreamConfiguration config) where TState : new()
        {
            IStreamProvider provider = context.ActivationServices.GetServiceByName<IStreamProvider>(config.StreamProviderName);
            return ActivatorUtilities.CreateInstance<RecoverableStream<TState>>(context.ActivationServices, provider);
        }
    }
}
