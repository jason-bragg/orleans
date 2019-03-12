using System;
using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Streams
{
    public interface ISiloPersistentStreamConfigurator : IComponentConfigurator<ISiloPersistentStreamConfigurator> {}

    public static class SiloPersistentStreamConfiguratorExtensions
    {
        public static ISiloPersistentStreamConfigurator UsePubSub(this ISiloPersistentStreamConfigurator configurator)
        {
            throw new NotImplementedException();
        }
        public static ISiloPersistentStreamConfigurator UseExplicitOnlyPubSub(this ISiloPersistentStreamConfigurator configurator)
        {
            throw new NotImplementedException();
        }
        public static ISiloPersistentStreamConfigurator UseImplicitSubscriptions(this ISiloPersistentStreamConfigurator configurator)
        {
            throw new NotImplementedException();
        }
        public static ISiloPersistentStreamConfigurator ConfigurePullingAgent(this ISiloPersistentStreamConfigurator configurator, Action<OptionsBuilder<StreamPullingAgentOptions>> configureOptions = null)
        {
            configurator.Configure<StreamPullingAgentOptions>(configureOptions);
            return configurator;
        }
        public static ISiloPersistentStreamConfigurator ConfigureLifecycle(this ISiloPersistentStreamConfigurator configurator, Action<OptionsBuilder<StreamLifecycleOptions>> configureOptions)
        {
            configurator.Configure<StreamLifecycleOptions>(configureOptions);
            return configurator;
        }

        public static ISiloPersistentStreamConfigurator ConfigurePartitionBalancing(this ISiloPersistentStreamConfigurator configurator, Func<IServiceProvider, string, IStreamQueueBalancer> factory)
        {
            return configurator.ConfigureComponent<IStreamQueueBalancer>(factory);
        }

        public static ISiloPersistentStreamConfigurator ConfigurePartitionBalancing<TOptions>(this ISiloPersistentStreamConfigurator configurator,
            Func<IServiceProvider, string, IStreamQueueBalancer> factory, Action<OptionsBuilder<TOptions>> configureOptions)
            where TOptions : class, new()
        {
            return configurator.ConfigureComponent<TOptions, IStreamQueueBalancer>(factory, configureOptions);
        }
    }
}
