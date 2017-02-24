using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Factory;
using Orleans.Runtime.Configuration;
using Orleans.TestingHost;
using TestExtensions;
using UnitTests.GrainInterfaces;
using Xunit;

namespace Tester
{
    public class UserFacetTests : TestClusterPerTest
    {
        public override TestCluster CreateTestCluster()
        {
            var options = new TestClusterOptions();
            options.ClusterConfiguration.UseStartupType<TestStartup>();
            return new TestCluster(options);
        }

        [Fact, TestCategory("BVT"), TestCategory("Functional"), TestCategory("Facet")]
        public async Task UserFacetHappyPath()
        {
            INamedFacetGrain grain = this.GrainFactory.GetGrain<INamedFacetGrain>(0);
            string[] names = await grain.GetNames();
            Assert.Equal(2, names.Length);
            Assert.Equal("A", names[0]);
            Assert.Equal("B", names[1]);
        }

        private class NamedFacet : INamedFacet
        {
            public NamedFacet(string name)
            {
                this.Name = name;
            }

            public string Name { get; }
        }

        private class NamedFacetFactory : IFactory<string, INamedFacet>
        {
            public INamedFacet Create(string key)
            {
                return new NamedFacet(key);
            }
        }

        private class TestStartup
        {
            public IServiceProvider ConfigureServices(IServiceCollection services)
            {
                services.AddSingleton<IFactory<string, INamedFacet>, NamedFacetFactory>();
                return services.BuildServiceProvider();
            }
        }
    }
}
