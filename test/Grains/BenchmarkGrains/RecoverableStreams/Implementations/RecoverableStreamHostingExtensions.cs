using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Runtime;
using Orleans.Streams;

namespace Orleans.Hosting
{
    public static class RecoverableStreamHostingExtensions
    {
        public static void UseRecoverableStreams(this ISiloHostBuilder builder)
        {
            builder.ConfigureServices(services =>
            {
                services.TryAddSingleton<IRecoverableStreamFactory, RecoverableStreamFactory>();
                services.TryAddSingleton(typeof(IAttributeToFactoryMapper<RecoverableStreamAttribute>), typeof(RecoverableStreamAttributeMapper));
            });
        }
    }
}
