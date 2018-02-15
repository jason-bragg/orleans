using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Storage;
using Orleans.Streams;
using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.TestingHost.Utils;
using TestExtensions;
using Xunit;

namespace Tester.StreamingTests
{
    public class PluggableQueueBalancerTestBase : OrleansTestingBase
    {
        private static Type QueueBalancerType = typeof(LeaseBasedQueueBalancerForTest);

        public static PersistentStreamOptions ConfigWithCustomBalancerType(PersistentStreamOptions options)
        {
            options.BalancerType = QueueBalancerType;
            return options;
        }

        public virtual async Task ShouldUseInjectedQueueBalancerAndBalanceCorrectly(BaseTestClusterFixture fixture, string streamProviderName, int siloCount, int totalQueueCount)
        {
            var leaseManager = fixture.GrainFactory.GetGrain<ILeaseManagerGrain>(streamProviderName);
            var responsibilityMap = await leaseManager.GetResponsibilityMap();
            //there should be one StreamQueueBalancer per silo
            Assert.Equal(responsibilityMap.Count, siloCount);
            var expectedResponsibilityPerBalancer = totalQueueCount / siloCount;
            foreach (var responsibility in responsibilityMap)
            {
                Assert.Equal(expectedResponsibilityPerBalancer, responsibility.Value);
            }
        }

        public class SiloBuilderConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder.ConfigureServices(services => services.AddTransient<LeaseBasedQueueBalancerForTest>());
            }
        }
    }
}
