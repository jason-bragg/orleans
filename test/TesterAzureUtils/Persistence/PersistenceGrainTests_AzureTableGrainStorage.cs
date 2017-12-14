using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Runtime.Configuration;
using Orleans.Hosting;
using Orleans.Configuration;
using Orleans.TestingHost;
using TestExtensions;

namespace Tester.AzureUtils.Persistence
{
    /// <summary>
    /// PersistenceGrainTests using AzureGrainStorage - Requires access to external Azure table storage
    /// </summary>
    [TestCategory("Persistence"), TestCategory("Azure")]
    public class PersistenceGrainTests_AzureTableGrainStorage : Base_PersistenceGrainTests_AzureStore, IClassFixture<PersistenceGrainTests_AzureTableGrainStorage.Fixture>
    {
        public class Fixture : BaseAzureTestClusterFixture
        {

            protected override TestCluster CreateTestCluster()
            {
                Guid serviceId = Guid.NewGuid();
                var options = new TestClusterOptions(initialSilosCount: 4);
                options.ClusterConfiguration.Globals.DataConnectionString = TestDefaultConfiguration.DataConnectionString;
                options.ClusterConfiguration.Globals.ServiceId = serviceId;

                return new TestCluster(options)
                    .UseSiloBuilderFactory<MySiloBuilderFactory>()
                    .UseClientBuilderFactory(ClientBuilderFactory);
            }

            private class MySiloBuilderFactory : ISiloBuilderFactory
            {
                private readonly SiloBuilderFactory factory = new SiloBuilderFactory();
                public ISiloHostBuilder CreateSiloBuilder(string siloName, ClusterConfiguration clusterConfiguration)
                {
                    return this.factory.CreateSiloBuilder(siloName, clusterConfiguration)
                        .ConfigureServices((c,s) => AddStorage(c,s, clusterConfiguration.Globals.ServiceId));
                }

                private void AddStorage(HostBuilderContext context, IServiceCollection serviceCollection, Guid serviceId)
                {
                    serviceCollection.AddAzureTableStorage("AzureStore", option =>
                        {
                            option.Name = "AzureStore"; // should not be needed if we change how the factory works
                            option.ServiceId = serviceId.ToString();
                            option.DataConnectionString = TestDefaultConfiguration.DataConnectionString;
                            option.DeleteStateOnClear = true;
                        });

                    // can also take options builder that allows chaining Configure and Bind calls
                    serviceCollection.AddAzureTableStorage("AzureStore1", optionsBuilder => 
                        optionsBuilder
                        .Bind(context.Configuration.GetSection("AzureStore1"))
                        .Configure(option =>
                        {
                            option.Name = "AzureStore1";
                            option.ServiceId = serviceId.ToString();
                            option.DataConnectionString = TestDefaultConfiguration.DataConnectionString;
                        }));
                    serviceCollection.AddAzureTableStorage("AzureStore2", option =>
                        {
                            option.Name = "AzureStore2";
                            option.ServiceId = serviceId.ToString();
                            option.DataConnectionString = TestDefaultConfiguration.DataConnectionString;
                        });
                    serviceCollection.AddAzureTableStorage("AzureStore3", option =>
                        {
                            option.Name = "AzureStore3";
                            option.ServiceId = serviceId.ToString();
                            option.DataConnectionString = TestDefaultConfiguration.DataConnectionString;
                        });
                }
            }
        }

        public PersistenceGrainTests_AzureTableGrainStorage(ITestOutputHelper output, Fixture fixture) : base(output, fixture)
        {
            fixture.EnsurePreconditionsMet();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_AzureTableGrainStorage_Delete()
        {
            await base.Grain_AzureStore_Delete();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_AzureTableGrainStorage_Read()
        {
            await base.Grain_AzureStore_Read();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_GuidKey_AzureTableGrainStorage_Read_Write()
        {
            await base.Grain_GuidKey_AzureStore_Read_Write();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_LongKey_AzureTableGrainStorage_Read_Write()
        {
            await base.Grain_LongKey_AzureStore_Read_Write();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_LongKeyExtended_AzureTableGrainStorage_Read_Write()
        {
            await base.Grain_LongKeyExtended_AzureStore_Read_Write();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_GuidKeyExtended_AzureTableGrainStorage_Read_Write()
        {
            await base.Grain_GuidKeyExtended_AzureStore_Read_Write();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_Generic_AzureTableGrainStorage_Read_Write()
        {
            StorageEmulatorUtilities.EnsureEmulatorIsNotUsed();

            await base.Grain_Generic_AzureStore_Read_Write();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_Generic_AzureTableGrainStorage_DiffTypes()
        {
            StorageEmulatorUtilities.EnsureEmulatorIsNotUsed();

            await base.Grain_Generic_AzureStore_DiffTypes();
        }

        [SkippableFact, TestCategory("Functional")]
        public async Task Grain_AzureTableGrainStorage_SiloRestart()
        {
            await base.Grain_AzureStore_SiloRestart();
        }
    }
}
