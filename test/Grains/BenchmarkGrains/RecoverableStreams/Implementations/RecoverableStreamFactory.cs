using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public class RecoverableStreamFactory : IRecoverableStreamFactory
    {
        public IRecoverableStream<TState> Create<TState>(IGrainActivationContext context, IRecoverableStreamConfiguration config) where TState : new()
        {
            IStreamProvider provider = context.ActivationServices.GetServiceByName<IStreamProvider>(config.StreamProviderName);
            Guid streamGuid = context.GrainIdentity.GetPrimaryKey(out string streamNamespace);
            StreamIdentity streamId = new StreamIdentity(streamGuid, streamNamespace);
            var stream = ActivatorUtilities.CreateInstance<RecoverableStream<TState>>(context.ActivationServices, provider, streamId);
            stream.Participate(context.ObservableLifecycle);
            return stream;
        }
    }
}
