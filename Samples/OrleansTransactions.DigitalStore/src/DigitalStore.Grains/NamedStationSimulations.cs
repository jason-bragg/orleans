using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace DigitalStore.Grains
{
    public class NamedStationSimulations : IKeyedServiceCollection<string, ISpaceStationSimulation>
    {
        public ISpaceStationSimulation GetService(IServiceProvider services, string key)
        {
            StationSettings stationSettings = services.GetServiceByName<StationSettings>(key);
            return (stationSettings != null)
                ? ActivatorUtilities.CreateInstance<SpaceStationSimulation>(services, stationSettings)
                : default(ISpaceStationSimulation);
        }
    }
}
