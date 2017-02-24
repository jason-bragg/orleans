
using System.Threading.Tasks;
using Orleans;
using Orleans.Facet;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    public class NamedFacetGrain : Grain, INamedFacetGrain
    {
        private readonly INamedFacet first;
        private readonly INamedFacet second;

        public NamedFacetGrain(
            [Facet("A")]
            INamedFacet first,
            [Facet("B")]
            INamedFacet second)
        {
            this.first = first;
            this.second = second;
        }

        public Task<string[]> GetNames()
        {
            return Task.FromResult(new[] {this.first.Name, this.second.Name});
        }
    }
}
