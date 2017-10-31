using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using DigitalStore.Interfaces;

namespace DigitalStore.Grains
{
    public class StarSystemGrain : Grain, IStarSystem
    {
        private List<StationDescription> stations;

        public override Task OnActivateAsync()
        {
            string systemName = this.GetPrimaryKeyString();
            this.stations = this.ServiceProvider.GetServices<IKeyedService<string,StationSettings>>()
                .Select(ks => ks.GetService(this.ServiceProvider))
                .Where(setting => setting.StarSystem == systemName)
                .Select(setting => new StationDescription() {  Name = setting.Name, Message = setting.Message })
                .ToList();
            return Task.CompletedTask;
        }

        Task<IList<StationDescription>> IStarSystem.GetStations()
        {
            return Task.FromResult<IList<StationDescription>>(this.stations);
        }
    }
}
