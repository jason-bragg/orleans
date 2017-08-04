using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Tester.StorageFacet.Abstractions;

namespace Tester.StorageFacet.Infrastructure
{
    public class NamedStorageFeatureConfig
    {
        public IDictionary<string, Type> StorageFeatureFactoryTypes { get; }

        public NamedStorageFeatureConfig(IDictionary<string, Type> storageFeatureFactoryTypes)
        {
            this.StorageFeatureFactoryTypes = storageFeatureFactoryTypes;
        }
    }

    public class NamedStorageFeatureFactory<TState> : INamedStorageFeatureFactory<TState>
    {
        private readonly NamedStorageFeatureConfig config;
        private readonly IServiceProvider services;

        public NamedStorageFeatureFactory(IServiceProvider services, NamedStorageFeatureConfig config)
        {
            this.services = services;
            this.config = config;
        }

        public IStorageFeature<TState> Create(string name, IStorageFeatureConfig cfg)
        {
            Type factoryType;
            IStorageFeatureFactory<TState> factory = null;
            if (string.IsNullOrEmpty(name))
            {
                factory = this.services.GetService<IStorageFeatureFactory<TState>>();
            } else if (this.config.StorageFeatureFactoryTypes.TryGetValue(name, out factoryType))
            {
                Type genericFactoryType = factoryType.MakeGenericType(typeof(TState));
                factory = this.services.GetRequiredService(genericFactoryType) as IStorageFeatureFactory<TState>;
            }
            if (factory != null) return factory.Create(cfg);
            throw new InvalidOperationException($"Provider with name {name} not found.");
        }
    }
}
