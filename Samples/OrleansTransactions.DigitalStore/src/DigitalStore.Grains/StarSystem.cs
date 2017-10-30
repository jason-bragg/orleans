using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using DigitalStore.Interfaces;

namespace DigitalStore.Grains
{
    public class StarSystem : Grain, IStarSystem
    {
        private List<StationDescription> stations;

        public override Task OnActivateAsync()
        {
            string systemName = this.GetPrimaryKeyString();
            this.stations = this.ServiceProvider.GetServices<IOptions<StationSettings>>()
                .Select(option => option.Value)
                .Where(setting => setting.StarSystem == systemName)
                .Select(setting => new StationDescription() {  Name = setting.Name, Message = setting.Message })
                .ToList();
            return Task.CompletedTask;
        }

        public Task<IList<StationDescription>> GetStations()
        {
            return Task.FromResult<IList<StationDescription>>(this.stations);
        }
    }

    public class MercuryPowerAndLightSettings : StationSettings
    {
        MercuryPowerAndLightSettings()
        {
            base.Name = "MercuryPowerAndLight";
            base.StarSystem = "Sol";
            base.Message = "We patented the sun.";
            base.StartPopulationInMillions = 2000;
            base.InitialStockInYears = 5;
            base.ConsumptionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 100 },
                { Product.Foods, 500 },
                { Product.Water, 1000 },
                { Product.Pharmaceuticals, 100 },
                { Product.Hardware, 1000 },
                { Product.Metals, 100 },
                { Product.Gasses, 500 },
            };
            base.ProductionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 30000 },
                { Product.Metals, 15000 },
            };
        }
    }

    public class EarthFoodAndDrugsSettings : StationSettings
    {
        EarthFoodAndDrugsSettings()
        {
            base.Name = "EarthFoodAndDrugs";
            base.StarSystem = "Sol";
            base.Message = "If you like Earth foods, you'll LOVE earth drugs!";
            base.StartPopulationInMillions = 14000;
            base.InitialStockInYears = 1;
            base.ConsumptionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 500 },
                { Product.Foods, 100 },
                { Product.Water, 100 },
                { Product.Pharmaceuticals, 100 },
                { Product.Hardware, 500 },
                { Product.Metals, 500 },
                { Product.Gasses, 100 },
            };
            base.ProductionRatios = new Dictionary<Product, float>
            {
                { Product.Foods, 3000 },
                { Product.Pharmaceuticals, 1500 },
            };
        }
    }

    public class LunaTechIndustriesSettings : StationSettings
    {
        LunaTechIndustriesSettings()
        {
            base.Name = "LunaTechIndustries";
            base.StarSystem = "Sol";
            base.Message = "We have crazy deals!";
            base.StartPopulationInMillions = 1000;
            base.InitialStockInYears = 2;
            base.ConsumptionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 500 },
                { Product.Foods, 500 },
                { Product.Water, 100 },
                { Product.Pharmaceuticals, 100 },
                { Product.Hardware, 100 },
                { Product.Metals, 500 },
                { Product.Gasses, 100 },
            };
            base.ProductionRatios = new Dictionary<Product, float>
            {
                { Product.Foods, 1500 },
                { Product.Hardware, 3000 },
            };
        }
    }

    public class MarsMetalWorksSettings : StationSettings
    {
        MarsMetalWorksSettings()
        {
            base.Name = "MarsMetalWorks";
            base.StarSystem = "Sol";
            base.Message = "New world metal heads!";
            base.StartPopulationInMillions = 3000;
            base.InitialStockInYears = 4;
            base.ConsumptionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 500 },
                { Product.Foods, 500 },
                { Product.Water, 1000 },
                { Product.Pharmaceuticals, 100 },
                { Product.Hardware, 100 },
                { Product.Metals, 100 },
                { Product.Gasses, 1000 },
            };
            base.ProductionRatios = new Dictionary<Product, float>
            {
                { Product.Metals, 3000 },
                { Product.Hardware, 1500 },
            };
        }
    }

    public class EuropanIceSettings : StationSettings
    {
        EuropanIceSettings()
        {
            base.Name = "EuropanIce";
            base.StarSystem = "Sol";
            base.Message = "It's cold here!";
            base.StartPopulationInMillions = 500;
            base.InitialStockInYears = 5;
            base.ConsumptionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 1500 },
                { Product.Foods, 500 },
                { Product.Water, 100 },
                { Product.Pharmaceuticals, 100 },
                { Product.Hardware, 100 },
                { Product.Metals, 100 },
                { Product.Gasses, 100 },
            };
            base.ProductionRatios = new Dictionary<Product, float>
            {
                { Product.Water, 3000 },
                { Product.Gasses, 1500 },
            };
        }
    }

    public class UranusGassesSettings : StationSettings
    {
        UranusGassesSettings()
        {
            base.Name = "UranusGasses";
            base.StarSystem = "Sol";
            base.Message = "Yes, we're still 13.";
            base.StartPopulationInMillions = 1000;
            base.InitialStockInYears = 5;
            base.ConsumptionRatios = new Dictionary<Product, float>
            {
                { Product.Energy, 1000 },
                { Product.Foods, 500 },
                { Product.Water, 100 },
                { Product.Pharmaceuticals, 100 },
                { Product.Hardware, 500 },
                { Product.Metals, 100 },
                { Product.Gasses, 100 },
            };
            base.ProductionRatios = new Dictionary<Product, float>
            {
                { Product.Water, 1500 },
                { Product.Gasses, 3000 },
            };
        }
    }
}
