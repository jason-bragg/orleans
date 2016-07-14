
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.TestingHost;
using Orleans.TestingHost.Utils;
using Tester;
using Tester.OrleansRxTests;
using UnitTests.Tester;
using Xunit;

namespace UnitTests.OrleansRxTests
{
    public class OrleansRxBroadcastTest : OrleansTestingBase, IClassFixture<OrleansRxBroadcastTest.Fixture>
    {
        private class Fixture : BaseTestClusterFixture
        {
            protected override TestCluster CreateTestCluster()
            {
                var options = new TestClusterOptions(2);
                return new TestCluster(options);
            }
        }

        [Fact, TestCategory("Functional"), TestCategory("OrleansRx")]
        public async Task Broadcast2MessagesTo2ListenersTest()
        {
            logger.Info("************************ Broadcast2MessagesTo2ListenersTest *********************************");

            // get server
            Guid serverId = Guid.NewGuid();
            IOrleansRxBroadcastServerGrain server = GrainClient.GrainFactory.GetGrain<IOrleansRxBroadcastServerGrain>(serverId);

            // get clients
            IOrleansRxBroadcastClientGrain client1 = GrainClient.GrainFactory.GetGrain<IOrleansRxBroadcastClientGrain>(Guid.NewGuid());
            IOrleansRxBroadcastClientGrain client2 = GrainClient.GrainFactory.GetGrain<IOrleansRxBroadcastClientGrain>(Guid.NewGuid());

            await client1.Listen(serverId);
            await client2.Listen(serverId);

            const int messageCount = 2;
            for (int i = 0; i < messageCount; i++)
            {
                await server.Tell(i.ToString());
                await server.Tell(string.Empty);
            }

            await TestingUtils.WaitUntilAsync(assertIsTrue => CheckCounters(messageCount, client1, client2, assertIsTrue), TimeSpan.FromSeconds(30));
        }

        private async Task<bool> CheckCounters(int expectedCount, IOrleansRxBroadcastClientGrain client1, IOrleansRxBroadcastClientGrain client2, bool assertIsTrue)
        {
            List<string> report1 = await client1.Report();
            List<string> report2 = await client2.Report();
            if (assertIsTrue)
            {
                // one stream per queue
                Assert.Equal(expectedCount, report1.Count);
                Assert.Equal(expectedCount, report2.Count);
            }
            else if (expectedCount != report1.Count ||
                     expectedCount != report2.Count)
            {
                return false;
            }
            return true;
        }

    }
}
