using System;
using System.Collections.Generic;
using DigitalStore.Interfaces;

namespace DigitalStore.Grains
{
    public interface ISpaceStationSimulation
    {
        bool SimulateYear(StationState state);
        bool CheckInventory(uint population, float consumptionRatio, uint quantity);
        StationState CreateInitialState();
        ulong GetPrice(StationState state, Product product);
    }

    public class StationState
    {
        public StationState()
        {
            foreach (Product product in Enum.GetValues(typeof(Product))) Inventory[product] = 0;
        }

        public uint PopulationInMillions { get; set; }
        public Dictionary<Product, uint> Inventory { get; set; } = new Dictionary<Product, uint>();
    }
}
