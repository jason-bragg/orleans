
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Hosting.Developer;

namespace Benchmarks.EventHub
{
    public class EventHubStreamBenchmark
    {
        private const string StreamProviderName = "Generator";
        private readonly List<ISiloHost> hosts = new List<ISiloHost>();
        private readonly int numSilos;
        private readonly int queueCount;

        public EventHubStreamBenchmark(int numSilos, int queueCount)
        {
            this.numSilos = numSilos;
            this.queueCount = queueCount;
        }

        public void SetupGenerated()
        {
            this.CreateSilos(ConfigureGenerated);
        }

        public async Task Run()
        {
            Stopwatch sw = Stopwatch.StartNew();
            await Task.WhenAll(this.hosts.Select(silo => silo.StartAsync()));
            sw.Stop();
            Console.WriteLine($"Cluster startup took {sw.ElapsedMilliseconds}ms");
            await Task.Delay(TimeSpan.FromMinutes(10));
            sw.Restart();
            await Task.WhenAll(this.hosts.Select(silo => silo.StopAsync()));
            await Task.WhenAll(this.hosts.Select(silo => silo.Stopped));
            sw.Stop();
            Console.WriteLine($"Cluster shutdown took {sw.ElapsedMilliseconds}ms");
            this.hosts.ForEach(h => h.Dispose());
            this.hosts.Clear();
        }

        private void ConfigureGenerated(ISiloHostBuilder siloBuilder)
        {
            siloBuilder
                .AddEventDataGeneratorStreams(
                            StreamProviderName,
                            b =>
                            {
                                b.Configure<EventDataGeneratorStreamOptions>(ob => ob.Configure(
                                options =>
                                {
                                    options.EventHubPartitionCount = this.queueCount;
                                }));
                                b.ConfigureCacheEviction(ob => ob.Configure(
                                options =>
                                {
                                    options.DataMaxAgeInCache = TimeSpan.FromMinutes(2);
                                    options.DataMinTimeInCache = TimeSpan.FromMinutes(1);
                                }));
                            });
        }

        private void CreateSilos(Action<ISiloHostBuilder> configure)
        {
            for (var i = 0; i < this.numSilos; ++i)
            {
                var primary = i == 0 ? null : new IPEndPoint(IPAddress.Loopback, 11111);
                var siloBuilder = new SiloHostBuilder()
                    .ConfigureDefaults()
                    .UseLocalhostClustering(
                        siloPort: 11111 + i,
                        gatewayPort: 30000 + i,
                        primarySiloEndpoint: primary)

//                    .ConfigureApplicationParts(parts =>
//                        parts.AddApplicationPart(typeof(IPingGrain).Assembly)
//                             .AddApplicationPart(typeof(PingGrain).Assembly))
                    .ConfigureServices(services => services.AddSingleton<TelemetryConsumer>())
                    .Configure<TelemetryOptions>(options => options.AddConsumer<TelemetryConsumer>())
                    .Configure<StatisticsOptions>(options =>
                    {
                        options.PerfCountersWriteInterval = TimeSpan.FromSeconds(5);
                    });

                configure(siloBuilder);
                this.hosts.Add(siloBuilder.Build());
            }
        }
    }
}