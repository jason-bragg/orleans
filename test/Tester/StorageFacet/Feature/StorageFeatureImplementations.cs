using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace Tester
{
    public class BlobStorageFeature<TState> : IStorageFeature<TState>
    {
        private readonly IStorageFeatureConfig config;

        public string Name => this.config.StateName;
        public TState State { get; set; }

        public BlobStorageFeature(IStorageFeatureConfig config)
        {
            this.config = config;
        }

        public Task SaveAsync()
        {
            Console.WriteLine($"I, {this.GetType().FullName}, did something to state with name {this.Name}");
            return Task.CompletedTask;
        }

        public string GetExtendedInfo()
        {
            return $"Blob:{this.Name}, StateType:{typeof(TState).Name}";
        }

        // factory function for grain activation
        public static IStorageFeature<TState> Create(IGrainActivationContext context, IStorageFeatureConfig config)
        {
            IStorageFeature<TState> storage = new BlobStorageFeature<TState>(config);
            storage.ParticipateInLifecycle(context.ObservableLifeCycle);
            return storage;
        }
    }

    public class TableStorageFeature<TState> : IStorageFeature<TState>, ILifecycleParticipant<IGrainLifeCycle>
    {
        private readonly IStorageFeatureConfig config;
        private bool activateCalled;

        public string Name => this.config.StateName;
        public TState State { get; set; }

        public TableStorageFeature(IStorageFeatureConfig config)
        {
            this.config = config;
        }

        public Task SaveAsync()
        {
            Console.WriteLine($"I, {this.GetType().FullName}, did something to state with name {this.Name}");
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

        // factory function for grain activation
        public static IStorageFeature<TState> Create(IGrainActivationContext context, IStorageFeatureConfig config)
        {
            IStorageFeature<TState> storage = new TableStorageFeature<TState>(config);
            storage.ParticipateInLifecycle(context.ObservableLifeCycle);
            return storage;
        }
    }

    public class StorageFeatureFactoryCollection<TState> : IStorageFeatureFactoryCollection<TState>
    {
        public Factory<IGrainActivationContext, IStorageFeatureConfig, IStorageFeature<TState>> GetService(string key)
        {
            // default
            if (string.IsNullOrEmpty(key))
            {
                return TableStorageFeature<TState>.Create;
            }
            if (key.StartsWith("Blob"))
            {
                return BlobStorageFeature<TState>.Create;
            }
            if (key.StartsWith("Table"))
            {
                return TableStorageFeature<TState>.Create;
            }
            throw new InvalidOperationException($"Provider with name {key} not found.");
        }

        Factory<IGrainActivationContext, IStorageFeatureConfig, object> IKeyedServiceCollection<string, Factory<IGrainActivationContext, IStorageFeatureConfig, object>>.GetService(string key)
        {
            return GetService(key);
        }
    }
}
