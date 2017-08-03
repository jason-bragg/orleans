using System;
using System.Reflection;
using Orleans;
using Orleans.Runtime;

namespace Tester
{
    [AttributeUsage(AttributeTargets.Parameter)]
    public class StorageFeatureAttribute : FacetAttribute, IStorageFeatureConfig
    {
        public string StorageProviderName { get; }

        public string StateName { get; }

        public StorageFeatureAttribute(string storageProviderName = null, string stateName = null)
        {
            this.StorageProviderName = storageProviderName;
            this.StateName = stateName;
        }
    }

    public class StorageFeatureParamiterFacetFactory : IParamiterFacetFactory<StorageFeatureAttribute>
    {
        public Factory<IGrainActivationContext, object> Create(ParameterInfo parameter, StorageFeatureAttribute attribute)
        {
            IStorageFeatureConfig config = attribute;
            // set state name to parameter name, if not already specified
            if (string.IsNullOrEmpty(config.StateName))
            {
                config = new StorageFeatureConfig(parameter.Name);
            }
            // use generic type args to define collection type.
            Type factorySelectorType = typeof(IStorageFeatureFactoryCollection<>).MakeGenericType(parameter.ParameterType.GetGenericArguments());
            return context => Create(factorySelectorType, attribute.StorageProviderName, config, context);
        }

        public Factory<IGrainActivationContext, object> Create(ParameterInfo parameter, FacetAttribute attribute)
        {
            return Create(parameter, attribute as StorageFeatureAttribute);
        }

        private object Create(Type factorySelectorType, string name, IStorageFeatureConfig config, IGrainActivationContext context)
        {
            var factorySelector = context.ActivationServices.GetService(factorySelectorType) as IStorageFeatureFactoryCollection;
            Factory<IGrainActivationContext, IStorageFeatureConfig, object> factory = factorySelector?.GetService(name);
            // if container was not scoped, we could cache the factory, but it is...
            return factory?.Invoke(context, config);
        }
    }
}
