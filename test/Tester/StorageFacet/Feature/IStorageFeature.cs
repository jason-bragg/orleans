using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace Tester
{
    public interface IStorageFeatureConfig
    {
        string StateName { get; }
    }

    public interface IStorageFeature<TState>
    {
        string Name { get; }

        TState State { get; set; }

        Task SaveAsync();

        string GetExtendedInfo();
    }

    /// <summary>
    /// Generic untyped storage feature factory collection
    /// Note: Used by genereric systems that may not know specific types.
    /// </summary>
    public interface IStorageFeatureFactoryCollection : IKeyedServiceCollection<string, Factory<IGrainActivationContext, IStorageFeatureConfig, object>>
    {
    }

    /// <summary>
    /// Type specific storage feature factory collection
    /// </summary>
    public interface IStorageFeatureFactoryCollection<TState> : IKeyedServiceCollection<string, Factory<IGrainActivationContext, IStorageFeatureConfig, IStorageFeature<TState>>>, IStorageFeatureFactoryCollection
    {
    }
}
