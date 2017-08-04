using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace Tester
{
    public interface IStorageFeatureFactory<TState>
    {
        IStorageFeature<TState> Create(IStorageFeatureConfig config);
    }

    public interface INamedStorageFeatureFactory<TState>
    {
        IStorageFeature<TState> Create(string name, IStorageFeatureConfig config);
    }

    public class BlobStorageFeatureFactory<TState> : IStorageFeatureFactory<TState>
    {
        private readonly IGrainActivationContext context;

        public BlobStorageFeatureFactory(IGrainActivationContext context)
        {
            this.context = context;
        }

        public IStorageFeature<TState> Create(IStorageFeatureConfig config)
        {
            IStorageFeature<TState> storage = new BlobStorageFeature<TState>(config);
            storage.ParticipateInLifecycle(context.ObservableLifeCycle);
            return storage;
        }
    }

    public class TableStorageFeatureFactory<TState> : IStorageFeatureFactory<TState>
    {
        private readonly IGrainActivationContext context;

        public TableStorageFeatureFactory(IGrainActivationContext context)
        {
            this.context = context;
        }

        public IStorageFeature<TState> Create(IStorageFeatureConfig config)
        {
            IStorageFeature<TState> storage = new TableStorageFeature<TState>(config);
            storage.ParticipateInLifecycle(context.ObservableLifeCycle);
            return storage;
        }
    }

    public class NamedStorageFeatureFactory<TState> : INamedStorageFeatureFactory<TState>
    {
        public readonly Dictionary<string, IStorageFeatureFactory<TState>> factories;

        public NamedStorageFeatureFactory(BlobStorageFeatureFactory<TState> blobFactory, TableStorageFeatureFactory<TState> tableFactory)
        {
            this.factories = new Dictionary<string, IStorageFeatureFactory<TState>>()
            {
                {"Blob", blobFactory},
                {"Table", tableFactory}
            };
        }

        public IStorageFeature<TState> Create(string name, IStorageFeatureConfig config)
        {
            IStorageFeatureFactory<TState> factory;
            if (this.factories.TryGetValue(name, out factory)) return factory.Create(config);
            throw new InvalidOperationException($"Provider with name {name} not found.");
        }
    }

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
}
