/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime.Configuration;
using Orleans.Streams;
using Orleans.TestingHost;
using Tester.TestStreamProviders.SimpleGeneratorStreamProvider;
using TestGrainInterfaces;
using TestGrains;
using UnitTests.Tester;

namespace UnitTests.StreamingTests
{
    [DeploymentItem("OrleansConfigurationForTesting.xml")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class StreamGeneratorProviderTests : UnitTestSiloHost
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        private const string StreamProviderName = GeneratedEventCollectorGrain.StreamProviderName;
        private const string StreamNamespace = GeneratedEventCollectorGrain.StreamNamespace;

        private readonly static SimpleGeneratorAdapterConfig Config = new SimpleGeneratorAdapterConfig
        {
            StreamProviderName = StreamProviderName,
            StreamNamespace = StreamNamespace,
            EventsInStream = 100,
            TotalQueueCount = 4,
        };

        public StreamGeneratorProviderTests()
            : base(new TestingSiloOptions
            {
                StartFreshOrleans = true,
                SiloConfigFile = new FileInfo("OrleansConfigurationForTesting.xml")
            })
        {
        }

        public override void AdjustForTest(ClusterConfiguration config)
        {
            // get initial settings from config
            var settings = Config.Flatten();

            // add queue balancer setting
            settings.Add(PersistentStreamProviderConfig.QUEUE_BALANCER_TYPE, StreamQueueBalancerType.DynamicClusterConfigDeploymentBalancer.ToString());

            // add pub/sub settting
            settings.Add(PersistentStreamProviderConfig.STREAM_PUBSUB_TYPE, StreamPubSubType.ImplicitOnly.ToString());

            // register stream provider
            config.Globals.RegisterStreamProvider<SimpleGeneratorStreamProvider>(StreamProviderName, settings);

            // make sure all node configs exist, for dynamic cluster queue balancer
            config.GetConfigurationForNode("Primary");
            config.GetConfigurationForNode("Secondary_1");

            base.AdjustForTest(config);
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            StopAllSilos();
        }

        [TestMethod, TestCategory("Functional"), TestCategory("Streaming")]
        public async Task ValidateGeneratedStreamsTest()
        {
            logger.Info("************************ ValidateGeneratedStreamsTest *********************************");
            await TestingUtils.WaitUntilAsync(CheckCounters, Timeout);
        }

        private async Task<bool> CheckCounters(bool assertIsTrue)
        {
            var reporter = GrainClient.GrainFactory.GetGrain<IGeneratedEventReporterGrain>(GeneratedEventCollectorGrain.ReporterId);

            var report = await reporter.GetReport(GeneratedEventCollectorGrain.StreamProviderName, GeneratedEventCollectorGrain.StreamNamespace);
            if (assertIsTrue)
            {
                // one stream per queue
                Assert.AreEqual(Config.TotalQueueCount, report.Count, "Stream count");
                foreach (int eventsPerStream in report.Values)
                {
                    Assert.AreEqual(Config.EventsInStream, eventsPerStream, "Events per stream");
                }
            }
            else if (Config.TotalQueueCount != report.Count ||
                     report.Values.Any(count => count != Config.EventsInStream))
            {
                return false;
            }
            return true;
        }
    }
}
