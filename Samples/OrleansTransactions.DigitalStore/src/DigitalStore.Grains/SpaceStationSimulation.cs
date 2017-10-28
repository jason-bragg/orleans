using System.Collections.Generic;
using DigitalStore.Interfaces;
using System;
using System.Linq;

namespace DigitalStore.Grains
{
    public class SpaceStationSimulation
    {
        private readonly StationProductionOptions production;

        public SpaceStationSimulation(StationProductionOptions production)
        {
            this.production = production;
        }

        public bool SimulateYear(StationState state)
        {
            // check to see if we have needed reasourse
            if(this.production.ConsumptionRatios.Any(kvp => !CheckInventory(state.PopulationInMillions, kvp.Value, state.Inventory[kvp.Key])))
            {
                return false;
            }
            // remove consumed products
            foreach (KeyValuePair<Product, float> kvp in this.production.ConsumptionRatios)
            {
                state.Inventory[kvp.Key] -= (uint)(state.PopulationInMillions * kvp.Value);
            }
            // add produced
            foreach (KeyValuePair<Product, float> kvp in this.production.ProductionRatios)
            {
                state.Inventory[kvp.Key] += (uint)(state.PopulationInMillions * kvp.Value);
            }
            return true;
        }

        bool CheckInventory(uint population, float consumptionRatio, uint quantity)
        {
            return population * consumptionRatio >= quantity;
        }

        public StationState CreateInitialState()
        {
            var state = new StationState
            {
                PopulationInMillions = production.StartPopulationInMillions
            };

            // calculate stockpiled inventory
            foreach (KeyValuePair<Product,float> kvp in production.ConsumptionRatios)
            {
                state.Inventory[kvp.Key] += (uint)(state.PopulationInMillions * kvp.Value * production.InitialStockInYears);
            }
            foreach (KeyValuePair<Product, float> kvp in production.ProductionRatios)
            {
                state.Inventory[kvp.Key] += (uint)(state.PopulationInMillions * kvp.Value * production.InitialStockInYears);
            }
            return state;
        }
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

    public class StationProductionOptions
    {
        // Intial population
        public uint StartPopulationInMillions { get; set; }
        // Years worth of intial products
        public uint InitialStockInYears { get; set; }
        // Products to produce by ratio of population.
        public Dictionary<Product, float> ProductionRatios { get; set; }
        // Products to consume by ratio of population.
        public Dictionary<Product, float> ConsumptionRatios { get; set; }
    }

    public class SolSimulationOptions
    {
        /// <summary>
        /// How much play time it takes for one in game year to pass.
        /// Default to 5 minute
        /// </summary>
        public TimeSpan OneYearSimulationTime { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// How often pirate attacks occur, on average.
        /// Defaults to 1 minute
        /// </summary>
        public TimeSpan AveratePirateAttackFrequency { get; set; } = TimeSpan.FromMinutes(1);
    }

}
