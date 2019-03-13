using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Providers.Streams.SimpleMessageStream;

namespace Orleans.Streams
{
    public interface ISimpleMessageStreamConfigurator : IComponentConfigurator<ISimpleMessageStreamConfigurator> { }
    public static class SimpleMessageStreamConfiguratorExtensions
    {
        public static ISimpleMessageStreamConfigurator UsePubSub(this ISimpleMessageStreamConfigurator configurator)
        {
            configurator.ConfigureComponent(PubSubSubscriptionRegistrar.Create);
            configurator.ConfigureComponent(PubSubSubscriptionManifest.Create);
            return configurator;
        }

        public static ISimpleMessageStreamConfigurator UseExplicitOnlyPubSub(this ISimpleMessageStreamConfigurator configurator)
        {
            configurator.ConfigureComponent(PubSubSubscriptionRegistrar.CreateUsingExplicitOnly);
            configurator.ConfigureComponent(PubSubSubscriptionManifest.CreateUsingExplicitOnly);
            return configurator;
        }

        public static ISimpleMessageStreamConfigurator UseImplicitSubscriptions(this ISimpleMessageStreamConfigurator configurator)
        {
            configurator.ConfigureComponent(PubSubSubscriptionRegistrar.CreateUsingImplicitOnly);
            configurator.ConfigureComponent(PubSubSubscriptionManifest.CreateUsingImplicitOnly);
            return configurator;
        }
    }

    public class SimpleMessageStreamConfigurator : NamedServiceConfigurator<ISimpleMessageStreamConfigurator>, ISimpleMessageStreamConfigurator
    {
        public SimpleMessageStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate)
            : base(name, configureDelegate)
        {
            ConfigureComponent<IStreamProvider>(SimpleMessageStreamProvider.Create);
        }
    }
}
