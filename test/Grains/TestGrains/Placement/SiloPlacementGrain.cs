using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Placement;
using Orleans.Runtime;
using UnitTests.GrainInterfaces.Placement;

namespace UnitTests.Grains.Placement
{
    [SiloPlacement]
    public class SiloPlacementGrain : Grain, ISiloPlacementGrain
    {
        public async Task<SiloAddress> GetSilo()
        {
            // totally cheating
            return (await GetActiveSilos()).FirstOrDefault(siloAddress => this.GetPrimaryKeyString().StartsWith(siloAddress.GetConsistentHashCode().ToString()));
        }

        private async Task<List<SiloAddress>> GetActiveSilos()
        {
            IManagementGrain mgmtGrain = GrainFactory.GetGrain<IManagementGrain>(0);

            Dictionary<SiloAddress, SiloStatus> statuses = await mgmtGrain.GetHosts(onlyActive: true);
            return statuses.Keys.ToList();
        }
    }
}
