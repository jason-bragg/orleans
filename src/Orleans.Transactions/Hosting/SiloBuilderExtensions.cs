using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Runtime;
using Orleans.Hosting;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure cluster to use an in-cluster transaction manager using defaults.
        /// </summary>
        public static ISiloHostBuilder UseInClusterTransactionManager(this ISiloHostBuilder builder)
        {
            return builder.UseInClusterTransactionManager(options => { });
        }

        /// <summary>
        /// Configure cluster to use an in-cluster transaction manager.
        /// </summary>
        public static ISiloHostBuilder UseInClusterTransactionManager(this ISiloHostBuilder builder, Action<TransactionsOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.UseInClusterTransactionManager(configureOptions));
        }

        /// <summary>
        /// Configure cluster to use an in-cluster transaction manager.
        /// </summary>
        public static IServiceCollection UseInClusterTransactionManager(this IServiceCollection services, Action<TransactionsOptions> configureOptions)
        {
            return services.UseInClusterTransactionManager(ob => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure cluster to use an in-cluster transaction manager.
        /// </summary>
        public static IServiceCollection UseInClusterTransactionManager(this IServiceCollection services,
            Action<OptionsBuilder<TransactionsOptions>> configureOptions)
        {
            configureOptions?.Invoke(services.AddOptions<TransactionsOptions>());
            return services.AddTransient<TransactionLog>()
                           .AddTransient<ITransactionManager, TransactionManager>()
                           .AddSingleton<TransactionServiceGrainFactory>()
                           .AddSingleton(sp => sp.GetRequiredService<TransactionServiceGrainFactory>().CreateTransactionManagerService());
        }

        /// <summary>
        /// Configure cluster to support the use of transactional state.
        /// </summary>
        public static ISiloHostBuilder UseTransactionalState(this ISiloHostBuilder builder)
        {
            return builder.ConfigureServices(services => services.UseTransactionalState());
        }

        /// <summary>
        /// Configure cluster to support the use of transactional state.
        /// </summary>
        public static IServiceCollection UseTransactionalState(this IServiceCollection services)
        {
            services.TryAddSingleton(typeof(ITransactionDataCopier<>), typeof(DefaultTransactionDataCopier<>));
            services.AddSingleton<IAttributeToFactoryMapper<TransactionalStateAttribute>, TransactionalStateAttributeMapper>();
            services.TryAddTransient<ITransactionalStateFactory, TransactionalStateFactory>();
            services.TryAddTransient<INamedTransactionalStateStorageFactory, NamedTransactionalStateStorageFactory>();
            services.AddTransient(typeof(ITransactionalState<>), typeof(TransactionalState<>));
            return services;
        }

    }
}
