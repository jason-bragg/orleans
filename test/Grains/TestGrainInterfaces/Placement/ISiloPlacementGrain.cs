using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace UnitTests.GrainInterfaces.Placement
{
    public interface ISiloPlacementGrain : IGrainWithStringKey
    {
        Task<SiloAddress> GetSilo();
    }
}
