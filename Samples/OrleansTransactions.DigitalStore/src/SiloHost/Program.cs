using DigitalStore.Grains;
using DigitalStore.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OrleansSiloHost
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                var host = await StartSilo();
                Console.WriteLine("Press Enter to terminate...");
                Console.ReadLine();

                await host.StopAsync();

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }

        private static async Task<ISiloHost> StartSilo()
        {
            // define the cluster configuration
            var config = ClusterConfiguration.LocalhostPrimarySilo();
            config.AddMemoryStorageProvider();

            var builder = new SiloHostBuilder()
                .UseConfiguration(config)
                .ConfigureServices(ConfigureServices)
                .AddApplicationPartsFromReferences(typeof(SpaceTraderGrain).Assembly)
                .ConfigureLogging(logging => logging.AddConsole())
                .Configure<SystemSimulationSettings>(sim => sim.OneYearSimulationTime = TimeSpan.FromMinutes(2));

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static void ConfigureServices(HostBuilderContext context, IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IKeyedServiceCollection<string, ISpaceStationSimulation>, NamedStationSimulations>();
            serviceCollection.AddTransient<SpaceStationSimulation>();
            serviceCollection.AddTransientNamedService<StationSettings, MercuryPowerAndLightSettings>("MercuryPowerAndLight");
            serviceCollection.AddTransientNamedService<StationSettings, EarthFoodAndDrugsSettings>("EarthFoodAndDrugs");
            serviceCollection.AddTransientNamedService<StationSettings, LunaTechIndustriesSettings>("LunaTechIndustries");
            serviceCollection.AddTransientNamedService<StationSettings, MarsMetalWorksSettings>("MarsMetalWorks");
            serviceCollection.AddTransientNamedService<StationSettings, EuropanIceSettings>("EuropanIce");
            serviceCollection.AddTransientNamedService<StationSettings, UranusGassesSettings>("UranusGasses");
        }

        // configuration, should be moved to json

        public class MercuryPowerAndLightSettings : StationSettings
        {
            public MercuryPowerAndLightSettings()
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
            public EarthFoodAndDrugsSettings()
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
            public LunaTechIndustriesSettings()
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
            public MarsMetalWorksSettings()
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
            public EuropanIceSettings()
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
            public UranusGassesSettings()
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
}
