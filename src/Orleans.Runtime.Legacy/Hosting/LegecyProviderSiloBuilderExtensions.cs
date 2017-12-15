using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Configuration;
using Orleans.Providers;
using Orleans.Providers.Legacy;

namespace Orleans.Hosting
{
    public static class LegecyProviderSiloBuilderExtensions
    {
        /// <summary>
        /// Configures the silo to use legacy Bootstrap providers
        /// </summary>
        public static ISiloHostBuilder UseBootstrapProviders(this ISiloHostBuilder builder)
        {
            return builder.ConfigureServices(services => services.UseBootstrapProviders());
        }

        /// <summary>
        /// Configures the silo to use legacy Bootstrap providers
        /// </summary>
        public static IServiceCollection UseBootstrapProviders(this IServiceCollection services)
        {
            services.AddSingleton<BootstrapProviderLifecycle>();
            services.AddFromExisting<ILifecycleParticipant<ISiloLifecycle>, BootstrapProviderLifecycle>();
            services.AddSingleton<BootstrapProviderManager>();
            services.AddFromExisting<IProviderManager, BootstrapProviderManager>();
            return services;
        }
    }
}
