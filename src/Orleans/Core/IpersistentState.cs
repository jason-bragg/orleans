
using Orleans.Facet;

namespace Orleans.Core
{
    public interface IPersistentState<TState> : IGrainFacet, IStorage
    {
        TState State { get; set; }
    }
}
