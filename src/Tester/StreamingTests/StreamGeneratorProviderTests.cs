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
using Tester.TestStreamProviders.Generator;
using TestGrainInterfaces;
using TestGrains;
using UnitTests.Tester;

namespace UnitTests.StreamingTests
{
    [DeploymentItem("OrleansConfigurationUnitTests.xml")]
    [DeploymentItem("OrleansProviders.dll")]
    [TestClass]
    public class StreamGeneratorProviderTests : UnitTestSiloHost
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        private const string StreamProviderName = GeneratedEventCollectorGrain.StreamProviderName;
        private const string StreamNamespace = GeneratedEventCollectorGrain.StreamNamespace;

        private readonly static SimpleGeneratorStreamProvider.AdapterFactory.Config Config = new SimpleGeneratorStreamProvider.AdapterFactory.Config
        {
            StreamProviderName = StreamProviderName,
            StreamNamespace = StreamNamespace,
            TotalStreamCountPerQueue = 5,
            TotalQueueCount = 4,
            TotalEventCountPerStream = 5,
        };

        private static void  MutateClusterConfig(ClusterConfiguration clusterConfiguration)
        {
            var settings = Config.Flatten();
            settings.Add(PersistentStreamProviderConfig.QUEUE_BALANCER_TYPE, StreamQueueBalancerType.DynamicClusterConfigDeploymentBalancer.ToString());
            settings.Add(PersistentStreamProviderConfig.STREAM_PUBSUB_TYPE, StreamPubSubType.ImplicitOnly.ToString());
            clusterConfiguration.Globals.RegisterStreamProvider<SimpleGeneratorStreamProvider>(StreamProviderName, settings);
        }

        public StreamGeneratorProviderTests()
            : base(new TestingSiloOptions
            {
                StartFreshOrleans = true,
                SiloConfigFile = new FileInfo("OrleansConfigurationUnitTests.xml"),
                ConfigMutator = MutateClusterConfig
            })
        {
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
                Assert.AreEqual(Config.TotalStreamCountPerQueue * Config.TotalQueueCount, report.Count, "Stream count");
                foreach (int eventsPerStream in report.Values)
                {
                    Assert.AreEqual(Config.TotalEventCountPerStream, eventsPerStream, "Events per stream");
                }
            }
            else if (Config.TotalStreamCountPerQueue * Config.TotalQueueCount != report.Count ||
                     report.Values.Any(count => count != Config.TotalEventCountPerStream))
            {
                return false;
            }
            return true;
        }
    }
}
