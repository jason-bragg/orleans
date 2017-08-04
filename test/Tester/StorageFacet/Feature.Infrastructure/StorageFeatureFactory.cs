using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Tester.StorageFacet.Abstractions;

namespace Tester.StorageFacet.Infrastructure
{
    public class StorageFeatureFactory<TStorageFeature, TState> : IStorageFeatureFactory<TState>
        where TStorageFeature : IStorageFeature<TState>
    {
        private readonly IGrainActivationContext context;

        public StorageFeatureFactory(IGrainActivationContext context)
        {
            this.context = context;
        }

        public IStorageFeature<TState> Create(IStorageFeatureConfig config)
        {
            IStorageFeature<TState> storage = context.ActivationServices.GetRequiredService<TStorageFeature>();
            (storage as IConfigurableStorageFeature)?.Configure(config);
            storage.ParticipateInLifecycle(context.ObservableLifeCycle);
            return storage;
        }
    }
}
