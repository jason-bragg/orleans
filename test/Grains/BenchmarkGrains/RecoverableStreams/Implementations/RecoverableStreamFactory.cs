using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Streams
{
    // TODO: Kind of weird that we have to pull the stream id off right here. Wondering if there should instead be an implicit version that does this and then a general factory/provider you can have injected to subscribe to arbitrary streams. Implicit version would hook in to activation lifecycle to start. Explicit version would need to be explicitly started but could decide to stop or auto stop on deactivation. 
    public class RecoverableStreamFactory : IRecoverableStreamFactory
    {
        public IRecoverableStream<TState> Create<TState>(IGrainActivationContext context, IRecoverableStreamConfiguration config) where TState : new()
        {
            IStreamProvider provider = context.ActivationServices.GetServiceByName<IStreamProvider>(config.StreamProviderName);
            Guid streamGuid = context.GrainIdentity.GetPrimaryKey(out string streamNamespace);
            StreamIdentity streamId = new StreamIdentity(streamGuid, streamNamespace);
            var stream = ActivatorUtilities.CreateInstance<RecoverableStream<TState>>(context.ActivationServices, provider, streamId);
            stream.Participate(context.ObservableLifecycle); // TODO: The recoverable stream should probably participate for itself
            return stream;
        }
    }
}
