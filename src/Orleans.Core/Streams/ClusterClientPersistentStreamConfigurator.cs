using System;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;

namespace Orleans.Streams
{
    public interface IClusterClientPersistentStreamConfigurator : IComponentConfigurator<IClusterClientPersistentStreamConfigurator> { }

    public static class ClusterClientPersistentStreamConfiguratorExtensions
    {
        public static IClusterClientPersistentStreamConfigurator ConfigureLifecycle(this IClusterClientPersistentStreamConfigurator configurator, Action<OptionsBuilder<StreamLifecycleOptions>> configureOptions)
        {
            configurator.Configure<StreamLifecycleOptions>(configureOptions);
            return configurator;
        }

        public static IClusterClientPersistentStreamConfigurator UsePubSub(this IClusterClientPersistentStreamConfigurator configurator)
        {
            throw new NotImplementedException();
        }

        public static IClusterClientPersistentStreamConfigurator UseExplicitOnlyPubSub(this IClusterClientPersistentStreamConfigurator configurator)
        {
            throw new NotImplementedException();
        }

        public static IClusterClientPersistentStreamConfigurator UseImplicitSubscriptions(this IClusterClientPersistentStreamConfigurator configurator)
        {
            throw new NotImplementedException();
        }
    }

    public class ClusterClientPersistentStreamConfigurator : NamedServiceConfigurator<IClusterClientPersistentStreamConfigurator>, IClusterClientPersistentStreamConfigurator
    {
        public ClusterClientPersistentStreamConfigurator(string name, IClientBuilder clientBuilder, Func<IServiceProvider, string, IQueueAdapterFactory> adapterFactory)
            : base(name, configureDelegate => clientBuilder.ConfigureServices(configureDelegate))
        {
            ConfigureComponent<IStreamProvider>(PersistentStreamProvider.Create);
            ConfigureComponent<ILifecycleParticipant<IClusterClientLifecycle>>(PersistentStreamProvider.ParticipateIn<IClusterClientLifecycle>);
            ConfigureComponent<IQueueAdapterFactory>(adapterFactory);
        }
    }
}
