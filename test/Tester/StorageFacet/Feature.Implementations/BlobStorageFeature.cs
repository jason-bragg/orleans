﻿
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Tester.StorageFacet.Abstractions;
using Tester.StorageFacet.Infrastructure;

namespace Tester.StorageFacet.Implementations
{
    public class BlobStorageFeature<TState> : IStorageFeature<TState>, IConfigurableStorageFeature
    {
        private IStorageFeatureConfig config;

        public string Name => this.config.StateName;
        public TState State { get; set; }

        public Task SaveAsync()
        {
            return Task.CompletedTask;
        }

        public string GetExtendedInfo()
        {
            return $"Blob:{this.Name}, StateType:{typeof(TState).Name}";
        }

        public void Configure(IStorageFeatureConfig cfg)
        {
            this.config = cfg;
        }
    }

    public class BlobStorageFeatureFactory<TState> : StorageFeatureFactory<BlobStorageFeature<TState>, TState>
    {
        public BlobStorageFeatureFactory(IGrainActivationContext context) : base(context)
        {
        }
    }

    public static class BlobStorageFeatureExtensions
    {
        public static void UseBlobStorageFeature(this IServiceCollection services)
        {
            services.AddTransient(typeof(BlobStorageFeatureFactory<>));
            services.AddTransient(typeof(BlobStorageFeature<>));
        }
    }
}
