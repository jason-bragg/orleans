using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.LeaseProviders;

namespace Orleans.Runtime.Development
{
    /// <summary>
    /// Developmen extensions for <see cref="ISiloBuilder"/> instances.
    /// For development and test purposes only - Not for production use.
    /// </summary>
    public static class DevelopmentSiloBuilderExtensions
    {
        /// <summary>
        /// Configure cluster to use an in-cluster transaction manager.
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static ISiloBuilder UseInMemoryLeaseProvider(this ISiloBuilder builder)
        {
            return builder.ConfigureServices(UseInMemoryLeaseProvider);
        }

        private static void UseInMemoryLeaseProvider(IServiceCollection services)
        {
            services.AddTransient<ILeaseProvider, InMemoryLeaseProvider>();
        }
    }
}
