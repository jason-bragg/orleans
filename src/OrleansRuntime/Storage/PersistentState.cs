
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core;
using Orleans.Facet;

namespace Orleans.Storage
{

    public class PersistentStateAttribute : FacetAttribute, IPersistentStateConfiguration
    {
        public string ProviderName { get; }
        public string Location { get; }

        public PersistentStateAttribute(string storageProviderName, string storageLocation)
        {
            this.ProviderName = storageProviderName;
            this.Location = storageLocation;
        }

        public object Create(IServiceProvider serviceProvider)
        {
            serviceProvider.GetRequiredService<IFactory<string,
        }
    }

    public interface IPersistentStateConfiguration
    {
        string ProviderName { get; }
        string Location { get; }
    }

    public class PersistentState<TState> : IPersistentState<TState>
    {
        private readonly IStorageProvider storageProvider;
        private IPersistentStateConfiguration config;

        public TState State { get; set; }

        public PersistentState(IStorageProvider storageProvider)
        {
            this.storageProvider = storageProvider;
        }

        void Configure(IPersistentStateConfiguration config)
        {
            this.config = config;
        }

        /*
        public Task Initialize(Grain grain)
        {
                        storageProvider.ReadStateAsync()
        }
        */

        public Task ClearStateAsync()
        {
            throw new NotImplementedException();
        }

        public Task ReadStateAsync()
        {
            throw new NotImplementedException();
        }

        public Task WriteStateAsync()
        {
            throw new NotImplementedException();
        }
    }
}
