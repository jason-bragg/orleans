using System;

namespace Orleans.Runtime
{
    [Serializable]
    internal class SiloPlacement : PlacementStrategy
    {
        internal static SiloPlacement Singleton { get; } = new SiloPlacement();
    }
}