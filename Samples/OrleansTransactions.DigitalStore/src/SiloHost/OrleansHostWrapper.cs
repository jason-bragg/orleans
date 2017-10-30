using System;
using System.Net;
using System.Reflection;
using Orleans.Runtime.Configuration;
using Orleans.Hosting;
using Microsoft.Extensions.DependencyInjection;
using DigitalStore.Grains;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;

namespace OrleansSiloHost
{
    internal class OrleansHostWrapper
    {
        private readonly ISiloHost _siloHost;
        private ILogger logger;

        public OrleansHostWrapper(ClusterConfiguration config, string[] args)
        {
            var siloArgs = SiloArgs.ParseArguments(args);
            if (siloArgs == null)
            {
                return;
            }

            if (siloArgs.DeploymentId != null)
            {
                config.Globals.DeploymentId = siloArgs.DeploymentId;
            }

            config.AddMemoryStorageProvider();

            this._siloHost = new SiloHostBuilder()
                .UseConfiguration(config)
                .ConfigureSiloName(siloArgs.SiloName)
                .ConfigureServices(ConfigureServices)
                .ConfigureLogging((logging) =>
                {
                    logging.SetMinimumLevel(LogLevel.Debug);
                    logging.AddConsole();
                })
                .AddApplicationPartsFromAppDomain()
                .Build();
        }
        
        public int Run()
        {
            if (_siloHost == null)
            {
                SiloArgs.PrintUsage();
                return 1;
            }

            try
            {
                _siloHost.StartAsync().Wait();
                ILogger logger = _siloHost.Services.GetRequiredService<ILoggerFactory>().CreateLogger("main");
                logger.LogInformation("Silo started");
                return 0;
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc);
                return 1;
            }
        }

        public int Stop()
        {
            if (_siloHost == null) return 0;
            try
            {
                _siloHost.StopAsync().Wait();
                this._siloHost.Stopped.Wait();
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc);
                return 1;
            }
            return 0;
        }

        private class SiloArgs
        {
            private SiloArgs(string siloName, string deploymentId)
            {
                this.DeploymentId = deploymentId;
                this.SiloName = siloName;
            }

            public static SiloArgs ParseArguments(string[] args)
            {
                string deploymentId = null;
                string siloName = null;

                foreach (var arg in args)
                {
                    if (arg.StartsWith("-") || arg.StartsWith("/"))
                    {
                        switch (arg.ToLowerInvariant())
                        {
                            case "/?":
                            case "/help":
                            case "-?":
                            case "-help":
                                // Query usage help. Return null so that usage is printed
                                return null;
                            default:
                                Console.WriteLine($"Bad command line arguments supplied: {arg}");
                                return null;
                        }
                    }
                    if (arg.Contains("="))
                    {
                        var parameters = arg.Split('=');
                        if (string.IsNullOrEmpty(parameters[1]))
                        {
                            Console.WriteLine($"Bad command line arguments supplied: {arg}");
                            return null;
                        }
                        switch (parameters[0].ToLowerInvariant())
                        {
                            case "deploymentid":
                                deploymentId = parameters[1];
                                break;
                            case "name":
                                siloName = parameters[1];
                                break;
                            default:
                                Console.WriteLine($"Bad command line arguments supplied: {arg}");
                                return null;
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Bad command line arguments supplied: {arg}");
                        return null;
                    }
                }
                // Default to machine name
                siloName = siloName ?? Dns.GetHostName();
                return new SiloArgs(siloName, deploymentId);
            }

            public static void PrintUsage()
            {
                string consoleAppName = typeof(SiloArgs).GetTypeInfo().Assembly.GetName().Name;
                Console.WriteLine(
                    $@"USAGE: {consoleAppName} [name=<siloName>] [deploymentId=<idString>] [/debug]
                Where:
                name=<siloName> - Name of this silo (optional)
                deploymentId=<idString> - Optionally override the deployment group this host instance should run in 
                (otherwise will use the one in the configuration");
            }

            public string SiloName { get; set; }
            public string DeploymentId { get; set; }
        }

        private void ConfigureServices(HostBuilderContext context, IServiceCollection serviceCollection)
        {
            serviceCollection.AddSingleton<IKeyedServiceCollection<string, ISpaceStationSimulation>, NamedStationSimulations>();
            serviceCollection.AddTransientNamedService<StationSettings, MercuryPowerAndLightSettings>("MercuryPowerAndLight");
            serviceCollection.AddTransientNamedService<StationSettings, EarthFoodAndDrugsSettings>("EarthFoodAndDrugs");
            serviceCollection.AddTransientNamedService<StationSettings, LunaTechIndustriesSettings>("LunaTechIndustries");
            serviceCollection.AddTransientNamedService<StationSettings, MarsMetalWorksSettings>("MarsMetalWorks");
            serviceCollection.AddTransientNamedService<StationSettings, EuropanIceSettings>("EuropanIce");
            serviceCollection.AddTransientNamedService<StationSettings, UranusGassesSettings>("UranusGasses");
        }
    }
}
