using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.CodeGeneration;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Messaging;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;
using Orleans.Streams;
using Orleans.Streams.Core;

namespace Orleans
{
    /// <summary>
    /// Builder used for creating <see cref="IClusterClient"/> instances.
    /// </summary>
    public class ClientBuilder : IClientBuilder
    {
        private readonly ServiceProviderBuilder serviceProviderBuilder = new ServiceProviderBuilder();
        private bool built;
        private ClientConfiguration clientConfiguration;

        /// <inheritdoc />
        public IClusterClient Build()
        {
            if (this.built) throw new InvalidOperationException($"{nameof(this.Build)} may only be called once per {nameof(ClientBuilder)} instance.");
            this.built = true;
            
            // Configure default services and build the container.
            this.serviceProviderBuilder.ConfigureServices(
                services =>
                {
                    services.TryAddSingleton(this.clientConfiguration ?? ClientConfiguration.StandardLoad());
                    services.TryAddFromExisting<IMessagingConfiguration, ClientConfiguration>();
                    services.TryAddFromExisting<ITraceConfiguration, ClientConfiguration>();
                });
            this.serviceProviderBuilder.ConfigureServices(AddDefaultServices);
            var serviceProvider = this.serviceProviderBuilder.BuildServiceProvider();

            serviceProvider.GetRequiredService<OutsideRuntimeClient>().ConsumeServices(serviceProvider);

            // Construct and return the cluster client.
            return serviceProvider.GetRequiredService<IClusterClient>();
        }

        /// <inheritdoc />
        public IClientBuilder UseConfiguration(ClientConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (this.clientConfiguration != null) throw new InvalidOperationException("Base configuration has already been specified and cannot be overridden.");
            this.clientConfiguration = configuration;
            return this;
        }

        /// <inheritdoc />
        public IClientBuilder ConfigureServices(Action<IServiceCollection> configureServices)
        {
            this.serviceProviderBuilder.ConfigureServices(configureServices);
            return this;
        }

        /// <inheritdoc />
        public IClientBuilder UseServiceProviderFactory<TContainerBuilder>(IServiceProviderFactory<TContainerBuilder> factory)
        {
            this.serviceProviderBuilder.UseServiceProviderFactory(factory);
            return this;
        }

        /// <inheritdoc />
        public IClientBuilder ConfigureContainer<TContainerBuilder>(Action<TContainerBuilder> configureContainer)
        {
            this.serviceProviderBuilder.ConfigureContainer(configureContainer);
            return this;
        }

        private static void AddDefaultServices(IServiceCollection services)
        {
            services.TryAddSingleton<TelemetryManager>();
            services.TryAddFromExisting<ITelemetryProducer, TelemetryManager>();
			services.AddLogging();
            //temporary change until runtime moved away from Logger
            services.TryAddSingleton(typeof(LoggerWrapper<>));
            services.TryAddSingleton<LoadedProviderTypeLoaders>();
            services.TryAddSingleton<StatisticsProviderManager>();
            services.TryAddSingleton<TypeMetadataCache>();
            services.TryAddSingleton<AssemblyProcessor>();
            services.TryAddSingleton<OutsideRuntimeClient>();
            services.TryAddFromExisting<IRuntimeClient, OutsideRuntimeClient>();
            services.TryAddFromExisting<IClusterConnectionStatusListener, OutsideRuntimeClient>();
            services.TryAddSingleton<GrainFactory>();
            services.TryAddSingleton<IGrainReferenceRuntime, GrainReferenceRuntime>();
            services.TryAddFromExisting<IGrainFactory, GrainFactory>();
            services.TryAddFromExisting<IInternalGrainFactory, GrainFactory>();
            services.TryAddFromExisting<IGrainReferenceConverter, GrainFactory>();
            services.TryAddSingleton<ClientProviderRuntime>();
            services.TryAddSingleton<IGatewayListProvider>(
                sp => ActivatorUtilities.CreateInstance<GatewayProviderFactory>(sp).CreateGatewayListProvider());
            services.TryAddSingleton<SerializationManager>();
            services.TryAddSingleton<MessageFactory>();

            // streaming
            services.TryAddSingleton<StreamProviderManager>();
            services.TryAddFromExisting<IStreamProviderManager, StreamProviderManager>();
            services.TryAddFromExisting<IStreamProviderRuntime, ClientProviderRuntime>();
            services.TryAddSingleton<ImplicitStreamSubscriberTable>();
            services.AddTransient<GrainBasedPubSubRuntime>();
            services.AddTransient<ImplicitStreamPubSub>();
            services.AddTransientNamedService<IStreamSubscriptionRegistrar, ImplicitStreamPubSub>(StreamPubSubType.ImplicitOnly.ToString());
            services.AddTransientNamedService<IStreamProducerRegistrar, ImplicitStreamPubSub>(StreamPubSubType.ImplicitOnly.ToString());
            services.AddTransientNamedService<IStreamSubscriptionRegistrar, GrainBasedPubSubRuntime>(StreamPubSubType.ExplicitGrainBasedOnly.ToString());
            services.AddTransientNamedService<IStreamProducerRegistrar, GrainBasedPubSubRuntime>(StreamPubSubType.ExplicitGrainBasedOnly.ToString());
            services.AddTransientNamedService<IStreamSubscriptionRegistrar, StreamPubSubImpl>(StreamPubSubType.ExplicitGrainBasedAndImplicit.ToString());
            services.AddTransientNamedService<IStreamProducerRegistrar, StreamPubSubImpl>(StreamPubSubType.ExplicitGrainBasedAndImplicit.ToString());
            services.TryAddSingleton<IKeyedServiceCollection<string,IStreamSubscriptionManager>, StreamSubscriptionManagerCollection>();

            services.TryAddSingleton<ClientStatisticsManager>();
            services.TryAddFromExisting<IProviderRuntime, ClientProviderRuntime>();
            services.TryAddSingleton<CodeGeneratorManager>();
            services.TryAddSingleton<IInternalClusterClient, ClusterClient>();
            services.TryAddFromExisting<IClusterClient, IInternalClusterClient>();
            services.TryAddSingleton(typeof(IKeyedServiceCollection<,>), typeof(KeyedServiceCollection<,>));
        }
    }
}