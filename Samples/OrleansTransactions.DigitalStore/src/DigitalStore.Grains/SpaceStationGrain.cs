using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans;
using Orleans.Runtime;
using DigitalStore.Interfaces;
using System;

namespace DigitalStore.Grains
{
    /// <summary>
    /// A non-player space station grain
    /// </summary>
    internal class SpaceStationGrain : Grain, ISpaceStation
    {
        Station station;
        StationState state;
        SpaceStationSimulation simulation;
        public override Task OnActivateAsync()
        {
            this.station = (Station)this.GetPrimaryKeyLong();
            this.simulation = this.ServiceProvider.GetServiceByKey<Station,SpaceStationSimulation>(station);
            this.state = this.simulation.CreateInitialState();
            return Task.CompletedTask;
        }

        Task<long> ISpaceStation.Buy(Product product, int quantity)
        {
            if (state.Inventory[product] <= quantity)
                throw new InvalidOperationException($"Station {this.station} does not have {quantity} {product} in stock.");
            int price = this.simulation.GetPrice(this.state, product);

        }

        Task<Dictionary<Product, Stock>> ISpaceStation.PeekAtInventory()
        {
            throw new System.NotImplementedException();
        }

        Task<long> ISpaceStation.Sell(Product product, int quantity)
        {
            throw new System.NotImplementedException();
        }
    }
}
