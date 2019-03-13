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
            configurator.ConfigureComponent(PubSubSubscriptionRegistrar.Create);
            configurator.ConfigureComponent(PubSubSubscriptionManifest.Create);
            return configurator;
        }

        public static IClusterClientPersistentStreamConfigurator UseExplicitOnlyPubSub(this IClusterClientPersistentStreamConfigurator configurator)
        {
            configurator.ConfigureComponent(PubSubSubscriptionRegistrar.CreateUsingExplicitOnly);
            configurator.ConfigureComponent(PubSubSubscriptionManifest.CreateUsingExplicitOnly);
            return configurator;
        }

        public static IClusterClientPersistentStreamConfigurator UseImplicitSubscriptions(this IClusterClientPersistentStreamConfigurator configurator)
        {
            configurator.ConfigureComponent(PubSubSubscriptionRegistrar.CreateUsingImplicitOnly);
            configurator.ConfigureComponent(PubSubSubscriptionManifest.CreateUsingImplicitOnly);
            return configurator;
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
