using System;
using System.Threading.Tasks;
using System.Diagnostics;
using Orleans.Hosting;
using Orleans.TestingHost;
using Orleans.Providers;
using Microsoft.Extensions.Configuration;
using Orleans;
using Orleans.Streams;

namespace Benchmarks.RecoverableStream
{
    public class RecoverableStreamBenchmark
    {
        private TestCluster host;

        public void MemorySetup()
        {
            var builder = new TestClusterBuilder();
            builder.AddSiloBuilderConfigurator<SiloMemoryStorageConfigurator>();
            builder.AddClientBuilderConfigurator<ClientMemoryStorageConfigurator>();
            this.host = builder.Build();
            this.host.Deploy();
        }

        public class SiloMemoryStorageConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder.AddMemoryStreams<DefaultMemoryMessageBodySerializer>("recoverable", b => b
                    .ConfigurePartitioning(1)
                    .ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly));
                hostBuilder.UseIRecoverableStreams();
            }
        }

        public class ClientMemoryStorageConfigurator : IClientBuilderConfigurator
        {
            public void Configure(IConfiguration configuration, IClientBuilder clientBuilder)
            {
                clientBuilder.AddMemoryStreams<DefaultMemoryMessageBodySerializer>("recoverable", b => b
                    .ConfigurePartitioning(1)
                    .ConfigureStreamPubSub(StreamPubSubType.ImplicitOnly));
            }
        }
        

        public async Task RunAsync()
        {
            bool running = true;
            Func<bool> isRunning = () => running;
            Task[] tasks = { Loop(isRunning), Task.Delay(TimeSpan.FromSeconds(10)) };
            await Task.WhenAny(tasks);
            running = false;
            await Task.WhenAll(tasks);
        }

        public async Task Loop(Func<bool> running)
        {
            Guid streamGuid = Guid.NewGuid();
            IStreamProvider provider = this.host.Client.GetStreamProvider("recoverable");
            IAsyncStream<int> testStream = provider.GetStream<int>(streamGuid, "test");
            IAsyncStream<int> otherStream = provider.GetStream<int>(streamGuid, "other");

            int count = 0;
            Stopwatch sw = Stopwatch.StartNew();
            while (running())
            {
                await testStream.OnNextAsync(count);
                await otherStream.OnNextAsync(count);
                count++;
            }
            sw.Stop();
            Console.WriteLine($"Wrote {count} events to two streams in {sw.ElapsedMilliseconds}ms.");
        }

        public void Teardown()
        {
            host.StopAllSilos();
        }
    }
}
