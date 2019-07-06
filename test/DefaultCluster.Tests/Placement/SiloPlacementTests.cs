using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Placement;
using TestExtensions;
using UnitTests.GrainInterfaces.Placement;
using Xunit;
using Xunit.Abstractions;

namespace DefaultCluster.Tests.Placement
{
    public class SiloPlacementTests : BaseTestClusterFixture
    {
        private readonly ITestOutputHelper output;

        public SiloPlacementTests(ITestOutputHelper output)
        {
            this.output = output;
            output.WriteLine("SiloPlacementTests - constructor");
        }

        [Fact, TestCategory("Placement"), TestCategory("Functional")]
        public async Task SiloPlacementShouldPlaceOnSilo()
        {
            List<SiloAddress> silos = await GetSilos();

            foreach(SiloAddress silo in silos)
            {
                SiloAddress actual = await GrainFactory.GetGrain<ISiloPlacementGrain>(silo,"Test").GetSilo();
                Assert.Equal(silo, actual);
            }
        }

        private async Task<List<SiloAddress>> GetSilos()
        {
            IManagementGrain mgmtGrain = this.GrainFactory.GetGrain<IManagementGrain>(0);
            return (await mgmtGrain.GetHosts(true)).Keys.ToList();
        }
    }
}
