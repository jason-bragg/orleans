using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace Tester
{
    public interface IStorageFeatureConfig
    {
        string StateName { get; }
    }

    public class StorageFeatureConfig : IStorageFeatureConfig
    {
        public StorageFeatureConfig(string stateName)
        {
            this.StateName = stateName;
        }

        public string StateName { get; }
    }

    public interface IStorageFeature<TState>
    {
        string Name { get; }

        TState State { get; set; }

        Task SaveAsync();

        string GetExtendedInfo();
    }
}
