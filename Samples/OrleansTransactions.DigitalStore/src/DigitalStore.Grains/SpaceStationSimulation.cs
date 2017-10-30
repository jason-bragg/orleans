using System;
using System.Collections.Generic;
using System.Linq;
using DigitalStore.Interfaces;

namespace DigitalStore.Grains
{
    public class SpaceStationSimulation : ISpaceStationSimulation
    {
        private readonly StationSettings production;
        StationState targetState;

        public SpaceStationSimulation(StationSettings production)
        {
            this.production = production;
            this.targetState = CreateInitialState();
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
            state.PopulationInMillions = (uint)(state.PopulationInMillions * 1.1);
            this.targetState = GetTargetStateByPopulaiton(state.PopulationInMillions);
            return true;
        }

        public bool CheckInventory(uint population, float consumptionRatio, uint quantity)
        {
            return population * consumptionRatio >= quantity;
        }

        public StationState CreateInitialState()
        {
            return GetTargetStateByPopulaiton(production.StartPopulationInMillions);
        }

        public StationState GetTargetStateByPopulaiton(uint population)
        {
            var state = new StationState
            {
                PopulationInMillions = population
            };

            // calculate stockpiled inventory
            foreach (KeyValuePair<Product, float> kvp in production.ConsumptionRatios)
            {
                state.Inventory[kvp.Key] += (uint)(state.PopulationInMillions * kvp.Value * production.InitialStockInYears);
            }
            foreach (KeyValuePair<Product, float> kvp in production.ProductionRatios)
            {
                state.Inventory[kvp.Key] += (uint)(state.PopulationInMillions * kvp.Value * production.InitialStockInYears);
            }
            return state;
        }

        public ulong GetPrice(StationState state, Product product)
        {
            uint targetQuantity = this.targetState.Inventory[product];
            uint currentQuantity = state.Inventory[product];
            if (currentQuantity == 0) return 300;
            return Math.Max((targetQuantity * 100) / currentQuantity , 300);
        }
    }
    
    public class StationSettings
    {
        public string Name { get; set; }
        public string StarSystem { get; set; }
        public string Message { get; set; }
        // Intial population
        public uint StartPopulationInMillions { get; set; }
        // Years worth of intial products
        public uint InitialStockInYears { get; set; }
        // Products to produce by ratio of population.
        public Dictionary<Product, float> ProductionRatios { get; set; }
        // Products to consume by ratio of population.
        public Dictionary<Product, float> ConsumptionRatios { get; set; }
    }

    public class SolSimulationSettings
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
        public TimeSpan AveragePirateAttackFrequency { get; set; } = TimeSpan.FromMinutes(1);
    }

}
