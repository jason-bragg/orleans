using Orleans;
using Orleans.TestingHost;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Streams;
using TestExtensions;
using UnitTests.Grains.ProgrammaticSubscribe;
using Xunit.Abstractions;
using Orleans.Hosting;

namespace Tester.StreamingTests.ProgrammaticSubscribeTests
{
    // this test suit mainly to prove subscriptions set up is decoupled from stream providers init
    // this test suit need to use TestClusterPerTest because each test has differnt provider config
    public class ProgrammaticSubscribeTestsWithDynamicProviderConfiguration : TestClusterPerTest
    {

        private ITestOutputHelper output;

        protected override void ConfigureTestCluster(TestClusterBuilder builder)
        {
            builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        }

        public class SiloConfigurator : ISiloBuilderConfigurator
        {
            public void Configure(ISiloHostBuilder hostBuilder)
            {
                hostBuilder.AddAzureBlobGrainStorageAsDefault()
                    .AddMemoryGrainStorage("PubSubStore");
            }
        }

        public ProgrammaticSubscribeTestsWithDynamicProviderConfiguration(ITestOutputHelper output)
        {
            this.output = output;
        }

        private async Task<List<StreamSubscription<Guid>>> SetupStreamingSubscriptionForStream<TGrainInterface>(SubscriptionManager subManager, IGrainFactory grainFactory,
            FullStreamIdentity streamIdentity, int grainCount)
            where TGrainInterface : IGrainWithGuidKey
        {
            //generate grain refs
            var subscriptions = new List<StreamSubscription<Guid>>();
            while (grainCount > 0)
            {
                var grainId = Guid.NewGuid();
                var subscription = await subManager.AddSubscription<TGrainInterface>(streamIdentity, grainId);
                subscriptions.Add(subscription);
                grainCount--;
            }

            return subscriptions;
        }
    }
}
