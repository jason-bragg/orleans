using System;
using System.Threading.Tasks;
using System.Linq;
using Orleans.Hosting;
using Orleans.TestingHost;
using Orleans.Runtime;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.Text;
using BenchmarkGrainInterfaces.Streams;
using BenchmarkGrains.Streams;
using Orleans.Providers;
using Orleans.Streams;
using TestExtensions;

namespace Benchmarks.Streams
{
    public class StreamsBenchmark
    {
        private TestCluster host;
        private int runs;
        private int concurrent;

        public StreamsBenchmark(int runs, int concurrent)
        {
            this.runs = runs;
            this.concurrent = concurrent;
        }

        public void MemorySetup()
        {
            var builder = new TestClusterBuilder(4);
            builder.AddSiloBuilderConfigurator<SiloMemoryConfigurator>();
            this.host = builder.Build();
            this.host.Deploy();
        }

        public void EventHubSetup()
        {
            var builder = new TestClusterBuilder(1);
            builder.AddSiloBuilderConfigurator<SiloEventHubConfigurator>();
            this.host = builder.Build();
            this.host.Deploy();
        }

        public class SiloMemoryConfigurator : ISiloBuilderConfigurator
        {
            private const int QueueCount = 4;
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder
                    .AddMemoryStreams<DefaultMemoryMessageBodySerializer>(WriteLoadGrain.StreamProviderName,b =>
                    {
                        b.ConfigurePartitioning(QueueCount);
                        b.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    });
            }
        }

        public class SiloEventHubConfigurator : ISiloBuilderConfigurator
        {
            private const string EHPath = "ehorleanstest";
            private const string EHConsumerGroup = "orleansnightly";
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder
                    .AddEventHubStreams(WriteLoadGrain.StreamProviderName,b =>
                    {
                        b.ConfigureEventHub(ob => ob.Configure(options =>
                        {
                            options.ConnectionString = TestDefaultConfiguration.EventHubConnectionString;
                            options.ConsumerGroup = EHConsumerGroup;
                            options.Path = EHPath;
                        }));
                        b.UseAzureTableCheckpointer(ob => ob.Configure(options =>
                        {
                            options.ConnectionString = TestDefaultConfiguration.DataConnectionString;
                            options.PersistInterval = TimeSpan.FromSeconds(10);
                        }));
                        b.ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly);
                    });
            }
        }

        public async Task RunAsync()
        {
            Console.WriteLine($"Cold Run.");
            await FullRunAsync();
            for(int i=0; i<runs; i++)
            {
                Console.WriteLine($"Warm Run {i+1}.");
                await FullRunAsync();
            }
        }

        private async Task FullRunAsync()
        {
            int runners = Math.Max(1,(int)Math.Sqrt(concurrent));
            Report[] reports = await Task.WhenAll(Enumerable.Range(0, runners).Select(i => RunAsync(i, runners)));
            Report finalReport = new Report();
            foreach (Report report in reports)
            {
                finalReport.Succeeded += report.Succeeded;
                finalReport.Failed += report.Failed;
                finalReport.Elapsed = TimeSpan.FromMilliseconds(Math.Max(finalReport.Elapsed.TotalMilliseconds, report.Elapsed.TotalMilliseconds));
            }
            Console.WriteLine($"{finalReport.Succeeded} events in {finalReport.Elapsed.TotalMilliseconds}ms.");
            Console.WriteLine($"{(int)(finalReport.Succeeded * 1000 / finalReport.Elapsed.TotalMilliseconds)} events per second.");
            Console.WriteLine($"{finalReport.Failed} events failed.");
        }

        public async Task<Report> RunAsync(int run, int concurrentPerRun)
        {
            IWriteLoadGrain load = this.host.Client.GetGrain<IWriteLoadGrain>(Guid.NewGuid());
            await load.Generate(run, concurrentPerRun);
            Report report = null;
            while (report == null)
            {
                await Task.Delay(TimeSpan.FromSeconds(30));
                report = await load.TryGetReport();
            }
            return report;
        }

        public void Teardown()
        {
            host.StopAllSilos();
        }
    }

    public class TelemetryConsumer : IMetricTelemetryConsumer
    {
        private readonly ILogger logger;

        public TelemetryConsumer(ILogger<TelemetryConsumer> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void TrackTrace(string message)
        {
            this.logger.LogInformation(message);
        }

        public void TrackTrace(string message, IDictionary<string, string> properties)
        {
            TrackTrace(PrintProperties(message, properties));
        }

        public void TrackTrace(string message, Severity severity)
        {
            TrackTrace(message);
        }

        public void TrackTrace(string message, Severity severity, IDictionary<string, string> properties)
        {
            TrackTrace(message, properties);
        }

        public void TrackMetric(string name, double value, IDictionary<string, string> properties = null)
        {
            TrackTrace(PrintProperties(name, value, properties));
        }

        public void TrackMetric(string name, TimeSpan value, IDictionary<string, string> properties = null)
        {
            TrackTrace(PrintProperties(name, value, properties));
        }

        public void IncrementMetric(string name)
        {
            TrackTrace(name + $" - Increment");
        }

        public void IncrementMetric(string name, double value)
        {
            TrackTrace(PrintProperties(name, value, null));
        }

        public void DecrementMetric(string name)
        {
            TrackTrace(name + $" - Decrement");
        }

        public void DecrementMetric(string name, double value)
        {
            TrackTrace(PrintProperties(name, value, null));
        }

        public void Flush()
        {
        }

        public void Close()
        {
        }

        private static string PrintProperties<TValue>(string message, TValue value, IDictionary<string, string> properties)
        {
            var sb = new StringBuilder(message + $" - Value: {value}");
            sb = AppendProperties(sb, properties);
            return sb.ToString();
        }

        private static string PrintProperties(string message, IDictionary<string, string> properties)
        {
            var sb = new StringBuilder(message);
            sb = AppendProperties(sb, properties);
            return sb.ToString();
        }

        private static StringBuilder AppendProperties(StringBuilder sb, IDictionary<string, string> properties)
        {
            if (properties == null || properties.Keys.Count == 0)
                return sb;

            sb.Append(" - Properties:");
            sb.Append(" ");
            sb.Append("{");

            foreach (var key in properties.Keys)
            {
                sb.Append(" ");
                sb.Append(key);
                sb.Append(" : ");
                sb.Append(properties[key]);
                sb.Append(",");
            }
            sb.Remove(sb.Length - 1, 1);
            sb.Append(" ");
            sb.Append("}");
            return sb;
        }
    }
}