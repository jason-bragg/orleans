using System;
using System.Reflection;
using Orleans;
using Orleans.Runtime;

namespace Tester
{
    [AttributeUsage(AttributeTargets.Parameter)]
    public class StorageFeatureAttribute : GrainConstructorFacetAttribute, IStorageFeatureConfig
    {
        private readonly string storageProviderName;
        private Type factorySelectorType;

        public string StateName { get; private set; }

        public StorageFeatureAttribute(string storageProviderName = null, string stateName = null)
        {
            this.storageProviderName = storageProviderName;
            this.StateName = stateName;
        }

        public override Factory<IGrainActivationContext, object> GetFactory(ParameterInfo parameter)
        {
            // set state name to parameter name, if not already specified
            if (string.IsNullOrEmpty(this.StateName))
            {
                this.StateName = parameter.Name;
            }
            // use generic type args to define collection type.
            this.factorySelectorType = typeof(IStorageFeatureFactoryCollection<>).MakeGenericType(parameter.ParameterType.GetGenericArguments());
            return Create;
        }

        public object Create(IGrainActivationContext context)
        {
            var factorySelector = context.ActivationServices.GetService(factorySelectorType) as IStorageFeatureFactoryCollection;
            Factory<IGrainActivationContext, IStorageFeatureConfig, object> factory = factorySelector?.GetService(this.storageProviderName);
            // if container was not scoped, we could cache the factory, but it is...
            return factory?.Invoke(context, this);
        }
    }
}
