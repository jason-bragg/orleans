using Orleans.Runtime;

namespace Orleans.Hosting
{
    public class GrainPlacementOptions
    {
        public string DefaultPlacementStrategy { get; set; } = DEFAULT_PLACEMENT_STRATEGY;
        public static readonly string DEFAULT_PLACEMENT_STRATEGY = nameof(RandomPlacement);
    }
}
