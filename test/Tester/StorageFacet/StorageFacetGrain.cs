using Orleans;
using System.Threading.Tasks;

namespace Tester
{
    public interface IStorageFacetGrain : IGrainWithIntegerKey
    {
        Task<string[]> GetNames();
        Task<string[]> GetExtendedInfo();
    }

    public class StorageFacetGrain : Grain, IStorageFacetGrain
    {
        private readonly IStorageFeature<string> first;
        private readonly IStorageFeature<string> second;

        public StorageFacetGrain(
            [StorageFeature("Blob", stateName: "FirstState")] IStorageFeature<string> first,
            [StorageFeature("Table")] IStorageFeature<string> second)
        {
            this.first = first;
            this.second = second;
        }

        public Task<string[]> GetNames()
        {
            return Task.FromResult(new[] { this.first.Name, this.second.Name });
        }

        public Task<string[]> GetExtendedInfo()
        {
            return Task.FromResult(new[] { this.first.GetExtendedInfo(), this.second.GetExtendedInfo() });
        }
    }
}
