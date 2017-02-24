
using Orleans.Facet;

namespace UnitTests.GrainInterfaces
{
    public interface INamedFacet : IGrainFacet
    {
        string Name { get; } 
    }
}
