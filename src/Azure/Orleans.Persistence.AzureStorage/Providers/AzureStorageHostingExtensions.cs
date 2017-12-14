using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Storage;
using System;
using System.Linq;

namespace Orleans.Hosting
{
    public static class AzureStorageHostingExtensions
    {
        public static ISiloHostBuilder AddAzureTableStorage(this ISiloHostBuilder hostBuilder, string name, Action<AzureTableStorageOptions> configureOptions)
        {
            return hostBuilder.ConfigureServices(services => services.AddAzureTableStorage(name, configureOptions));
        }

        public static ISiloHostBuilder AddAzureTableStorage(this ISiloHostBuilder hostBuilder, string name, Action<OptionsBuilder<AzureTableStorageOptions>> configureOptions = null)
        {
            return hostBuilder.ConfigureServices(services => services.AddAzureTableStorage(name, configureOptions));
        }

        public static IServiceCollection AddAzureTableStorage(this IServiceCollection services, string name, Action<AzureTableStorageOptions> configureOptions)
        {
            return services.AddAzureTableStorage(name, ob => ob.Configure(configureOptions));
        }

        public static IServiceCollection AddAzureTableStorage(this IServiceCollection services, string name, Action<OptionsBuilder<AzureTableStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<AzureTableStorageOptions>(name));
            services.TryAddKeyedServiceCollection();

            services.AddNamedSiloLifecycleParticipant(name, AzureTableGrainStorageFactory.Create);

            // TODO: instead of named silo lifetime participant, use normal keyed services 
            // (but add an overload that accept a factory). This way we decouple registration from default keyed service implementation
            // (and could work with any impl of keyed service collection). Also, it's up to each provider to decide whether it wants
            // to participate in which lifetimes, and not bundled with the keyed service infrastructure.

            // Code would look something like this:
            // services.AddSingletonNamedService<IGrainStorage, AzureTableGrainStorage>(name);
            // previous line would efectively be a decorator of this more general method (but most providers won't need it if we go with the constructor I mentioned):
            // services.AddSingletonNamedService<IGrainStorage>(name, (sp, key) => ActivatorUtilities.CreateInstance<AzureTableGrainStorage>(sp, key));

            // Then register this keyed service as a lifetime participant separately:
            // services.AddSingleton<ILifecycleParticipant<ISiloLifecycle>>(sp => (AzureTableGrainStorage)sp.GetServiceByName<IGrainStorage>(name));

            return services;
        }

        // TODO: can be simplified
        private static void TryAddKeyedServiceCollection(this IServiceCollection services)
        {
            if (!services.Any(s => s.ServiceType == typeof(KeyedSiloLifecycleParticipantCollection<string, IGrainStorage>)))
            {
                services.AddSingleton<KeyedSiloLifecycleParticipantCollection<string, IGrainStorage>>();
                services.AddFromExisting<ILifecycleParticipant<ISiloLifecycle>, KeyedSiloLifecycleParticipantCollection<string, IGrainStorage>>();
            }
        }
    }
}
