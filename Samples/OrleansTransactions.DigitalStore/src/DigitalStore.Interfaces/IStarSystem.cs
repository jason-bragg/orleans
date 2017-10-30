using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace DigitalStore.Interfaces
{
    public interface IStarSystem : IGrainWithStringKey
    {
        Task<IList<StationDescription>> GetStations();
    }

    public class StationDescription
    {
        public string Name { get; set; }
        public string Message { get; set; }
    }
}
