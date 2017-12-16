using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Storage;

namespace Orleans.Transactions
{
    public class NamedTransactionalStateStorageFactory : INamedTransactionalStateStorageFactory
    {
        private readonly IGrainActivationContext context;
        private readonly ILoggerFactory loggerFactory;

        public NamedTransactionalStateStorageFactory(IGrainActivationContext context, ILoggerFactory loggerFactory)
        {
            this.context = context;
            this.loggerFactory = loggerFactory;
        }
        public ITransactionalStateStorage<TState> Create<TState>(string storageName)
            where TState : class, new()
        {
            // Try to get ITransactionalStateStorage from factory
            ITransactionalStateStorageFactory factory = string.IsNullOrEmpty(storageName)
                ? this.context.ActivationServices.GetService<ITransactionalStateStorageFactory>()
                : this.context.ActivationServices.GetServiceByName<ITransactionalStateStorageFactory>(storageName);
            if (factory != null) return factory.Create<TState>();

            // Else try to get storage provider and wrap it
            IStorageProvider storageProvider = string.IsNullOrEmpty(storageName)
                ? this.context.ActivationServices.GetService<IStorageProvider>()
                : this.context.ActivationServices.GetServiceByName<IStorageProvider>(storageName);
            if (storageProvider != null) return new TransactionalStateStorageProviderWrapper<TState>(storageProvider, context, this.loggerFactory);
            throw (string.IsNullOrEmpty(storageName))
                ? new InvalidOperationException($"No default {nameof(ITransactionalStateStorageFactory)} nor {nameof(IStorageProvider)} was found while attempting to create transactional state storage.")
                : new InvalidOperationException($"No {nameof(ITransactionalStateStorageFactory)} nor {nameof(IStorageProvider)} with the name {storageName} was found while attempting to create transactional state storage.");
        }
    }
}
