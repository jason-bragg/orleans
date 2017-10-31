using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Runtime;
using DigitalStore.Interfaces;

namespace DigitalStore.Grains
{
    /// <summary>
    /// A non-player space station grain
    /// </summary>
    public class SpaceStationGrain : Grain, ISpaceStation
    {
        private string station;
        private StationState state;
        private ISpaceStationSimulation simulation;
        private IDisposable yearlyTimer;

        public override Task OnActivateAsync()
        {
            this.station = this.GetPrimaryKeyString();
            this.simulation = this.ServiceProvider.GetServiceByName<ISpaceStationSimulation>(station);
            this.state = this.simulation.CreateInitialState();
            IOptions<SystemSimulationSettings> settings = this.ServiceProvider.GetRequiredService<IOptions<SystemSimulationSettings>>();
            this.yearlyTimer = this.RegisterTimer(YearlyTick, null, settings.Value.OneYearSimulationTime, settings.Value.OneYearSimulationTime);
            return Task.CompletedTask;
        }

        Task<ulong> ISpaceStation.Buy(Product product, uint quantity)
        {
            if (state.Inventory[product] <= quantity)
                throw new InvalidOperationException($"Station {this.station} does not have {quantity} {product} in stock.");
            Stock item = new Stock(0, this.simulation.GetPrice(this.state, product));
            return Task.FromResult((ulong)(item.BuyPrice * quantity));
        }

        Task<Dictionary<Product, Stock>> ISpaceStation.PeekAtInventory()
        {
            return Task.FromResult(this.state.Inventory.Keys.
                ToDictionary(p => p, p => new Stock(this.state.Inventory[p], this.simulation.GetPrice(this.state, p))));
        }

        Task<ulong> ISpaceStation.Sell(Product product, uint quantity)
        {
            if (state.Inventory[product] <= quantity)
                throw new InvalidOperationException($"Station {this.station} does not have {quantity} {product} in stock.");
            Stock item = new Stock(0, this.simulation.GetPrice(this.state, product));
            return Task.FromResult((ulong)(item.SellPrice * quantity));
        }

        private Task YearlyTick(object obj)
        {
            this.simulation.SimulateYear(this.state);
            return Task.CompletedTask;
        }
    }
}
