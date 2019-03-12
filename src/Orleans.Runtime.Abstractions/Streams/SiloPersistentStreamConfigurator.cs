using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public class SiloPersistentStreamConfigurator : NamedServiceConfigurator<ISiloPersistentStreamConfigurator>, ISiloPersistentStreamConfigurator
    {
        public SiloPersistentStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate, Func<IServiceProvider, string, IQueueAdapterFactory> adapterFactory)
            : base(name, configureDelegate)
        {
            ConfigureComponent<IStreamProvider>(PersistentStreamProvider.Create);
            ConfigureComponent<ILifecycleParticipant<ISiloLifecycle>>(PersistentStreamProvider.ParticipateIn<ISiloLifecycle>);
            ConfigureComponent<IQueueAdapterFactory>(adapterFactory);
        }
    }
}
