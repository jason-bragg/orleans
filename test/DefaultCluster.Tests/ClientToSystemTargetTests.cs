using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Orleans;
using Orleans.Core;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Providers;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
using TestExtensions;
using UnitTests.Grains;

namespace DefaultCluster.Tests
{
    [TestCategory("BVT"), TestCategory("ClientAddressable"), TestCategory("Functional")]
    public class ClientToSystemTargetTests : IClassFixture<ClientToSystemTargetTests.Fixture>
    {
        private const int SiloCount = 2;
        private readonly Fixture fixture;
        private readonly ITestOutputHelper output;

        public ClientToSystemTargetTests(Fixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
        }

        public class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions(SiloCount);
                // ensure that at least one silo has no gateway
                // options.ClusterConfiguration.Overrides.Values.First().ProxyGatewayEndpoint = null;
                Type clientTestGrainServiceType = typeof(ClientTestGrainService);
                options.ClusterConfiguration.Globals.RegisterGrainService(clientTestGrainServiceType.Name, clientTestGrainServiceType.AssemblyQualifiedName);
                return new TestCluster(options);
            }
        }

        [Fact]
        public async Task SystemTargetsToClientTest()
        {
            IClientTestGrainServiceLookupGrain lookup = this.fixture.GrainFactory.GetGrain<IClientTestGrainServiceLookupGrain>(0);

            // wait until all of the services are available
            await TestingUtils.WaitUntilAsync(assertIsTrue => this.CheckServiceCount(SiloCount, lookup, assertIsTrue), TimeSpan.FromSeconds(10));

            // lookup service, should be 1 per silo
            List<IClientTestGrainService> services = await lookup.Lookup();

            // bind extension to receive callback
            IProviderRuntime runtime = fixture.Client.ServiceProvider.GetRequiredService<IProviderRuntime>();
            Tuple<ClientTestCallback, IClientTestCallback> tup = await runtime.BindExtension<ClientTestCallback, IClientTestCallback>(
                () => new ClientTestCallback());

            // tell all services to call me
            await Task.WhenAll(services.Select(s => s.CallMe(tup.Item2, Guid.NewGuid())));

            // wait for callbacks
            await TestingUtils.WaitUntilAsync(assertIsTrue => this.CheckCallback(SiloCount, tup.Item1, assertIsTrue), TimeSpan.FromSeconds(2));
        }

        private async Task<bool> CheckServiceCount(int expectedCount, IClientTestGrainServiceLookupGrain lookup, bool assertIsTrue)
        {
            List<IClientTestGrainService> services = await lookup.Lookup();

            if (assertIsTrue)
            {
                // one stream per queue
                Assert.Equal(expectedCount, services.Count);
            }
            else if (expectedCount != services.Count)
            {
                return false;
            }
            return true;
        }

        private Task<bool> CheckCallback(int expectedCount, ClientTestCallback callback, bool assertIsTrue)
        {
            if (assertIsTrue)
            {
                // one stream per queue
                Assert.Equal(expectedCount, callback.Called.Count);
            }
            else if (expectedCount != callback.Called.Count)
            {
                return Task.FromResult(false);
            }
            return Task.FromResult(true);
        }
    }

    public class ClientTestGrainService : GrainService, IClientTestGrainService
    {
        private readonly IGrainIdentity id;
        private IClientTestGrainServiceLookupGrain lookupGrain;

        public ClientTestGrainService(IGrainIdentity id, Silo silo, IGrainServiceConfiguration config)
            : base(id, silo, config)
        {
            this.id = id;
        }

        public Task CallMe(IClientTestCallback clientCallback, Guid id)
        {
            this.Logger.Info($"{nameof(ClientTestGrainService)} with ID {this.id} received callme.");
            DelayedCallback(clientCallback, id).Ignore();
            return Task.CompletedTask;
        }

        /// <summary>Invoked upon initialization of the service</summary>
        public override Task Init(IServiceProvider serviceProvider)
        {
            this.Logger.Info($"{nameof(ClientTestGrainService)} initialized with ID {this.id}");
            this.lookupGrain = serviceProvider.GetRequiredService<IGrainFactory>().GetGrain<IClientTestGrainServiceLookupGrain>(0);
            return Task.CompletedTask;
        }

        protected override async Task StartInBackground()
        {
            await Task.Delay(TimeSpan.FromSeconds(2), this.StoppedCancellationTokenSource.Token);
            await this.lookupGrain.Register(this.AsReference<IClientTestGrainService>());
            this.Logger.Info($"{nameof(ClientTestGrainService)} with ID {this.id} register.");
        }

        private async Task DelayedCallback(IClientTestCallback clientCallback, Guid id)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), this.StoppedCancellationTokenSource.Token);
            await clientCallback.ReturnCall(id);
            this.Logger.Info($"{nameof(ClientTestGrainService)} with ID {this.id} made callback.");
        }
    }
}
