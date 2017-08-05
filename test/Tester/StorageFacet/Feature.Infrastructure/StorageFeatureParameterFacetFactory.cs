using System;
using System.Reflection;
using Orleans;
using Orleans.Runtime;
using Tester.StorageFacet.Abstractions;

namespace Tester.StorageFacet.Infrastructure
{
    public class StorageFeatureParameterFacetFactory : IParameterFacetFactory<StorageFeatureAttribute>
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
            Type factorySelectorType = typeof(INamedStorageFeatureFactory<>).MakeGenericType(parameter.ParameterType.GetGenericArguments());
            IFactoryBridge bridge = Activator.CreateInstance(typeof(FactoryBridge<>).MakeGenericType(parameter.ParameterType.GetGenericArguments())) as IFactoryBridge;
            return context => Create(factorySelectorType, bridge, attribute.StorageProviderName, config, context);
        }

        private object Create(Type factorySelectorType, IFactoryBridge bridge, string name, IStorageFeatureConfig config, IGrainActivationContext context)
        {
            bridge.Factory = context.ActivationServices.GetService(factorySelectorType);
            return bridge.Create(name, config);
        }

        private interface IFactoryBridge
        {
            object Factory { set; }
            object Create(string name, IStorageFeatureConfig config);
        }

        private class FactoryBridge<TState> : IFactoryBridge
        {
            public object Factory { private get; set; }

            public object Create(string name, IStorageFeatureConfig config)
            {
                return (Factory as INamedStorageFeatureFactory<TState>)?.Create(name, config);
            }
        }
    }

}
