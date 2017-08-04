using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using Tester.StorageFacet.Abstractions;
using Tester.StorageFacet.Infrastructure;

namespace Tester.StorageFacet.Implementations
{
    public class TableStorageFeature<TState> : IStorageFeature<TState>, ILifecycleParticipant<IGrainLifeCycle>, IConfigurableStorageFeature
    {
        private IStorageFeatureConfig config;
        private bool activateCalled;

        public string Name => this.config.StateName;
        public TState State { get; set; }

        public Task SaveAsync()
        {
            return Task.CompletedTask;
        }

        public string GetExtendedInfo()
        {
            return $"Table:{this.Name}-ActivateCalled:{this.activateCalled}, StateType:{typeof(TState).Name}";
        }

        public Task LoadState()
        {
            this.activateCalled = true;
            return Task.CompletedTask;
        }

        public void Participate(IGrainLifeCycle lifecycle)
        {
            lifecycle.Subscribe(GrainLifecyleStage.SetupState, LoadState);
        }

        public void Configure(IStorageFeatureConfig cfg)
        {
            this.config = cfg;
        }
    }

    public class TableStorageFeatureFactory<TState> : StorageFeatureFactory<TableStorageFeature<TState>, TState>
    {
        public TableStorageFeatureFactory(IGrainActivationContext context) : base(context)
        {
        }
    }

    public static class TableStorageFeatureExtensions
    {
        public static void UseTableStorageFeature(this IServiceCollection services)
        {
            services.AddTransient(typeof(TableStorageFeatureFactory<>));
            services.AddTransient(typeof(TableStorageFeature<>));
        }
    }
}
