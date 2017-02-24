
using System.Threading.Tasks;
using Orleans;

namespace UnitTests.GrainInterfaces
{
    public interface INamedFacetGrain : IGrainWithIntegerKey
    {
        Task<string[]> GetNames();
    }
}
