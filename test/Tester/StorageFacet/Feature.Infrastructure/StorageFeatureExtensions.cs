using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Tester.StorageFacet.Abstractions;
using Tester.StorageFacet.Implementations;

namespace Tester.StorageFacet.Infrastructure
{
    public static class StorageFeatureExtensions
    {
        public static void UseStorageFeature(this IServiceCollection services, IDictionary<string, Type> storageFeatureFactoryTypeMap)
        {
            // Add configuration for named storage features
            services.AddSingleton(sp => new NamedStorageFeatureConfig(storageFeatureFactoryTypeMap));

            // storage feature factory infrastructure
            services.AddScoped(typeof(INamedStorageFeatureFactory<>), typeof(NamedStorageFeatureFactory<>));

            // storage feature facet attribute
            services.AddSingleton(typeof(IParameterFacetFactory<StorageFeatureAttribute>), typeof(StorageFeatureParameterFacetFactory));
        }

        public static void UseAsDefaultStorageFeature(this IServiceCollection services, Type factoryType)
        {
            if (factoryType.GetInterface(typeof(IStorageFeatureFactory<>).Name)==null) throw new ArgumentException(nameof(factoryType));
            services.AddScoped(typeof(IStorageFeatureFactory<>), factoryType);
        }
    }
}
