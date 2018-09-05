﻿
using Xunit.Abstractions;
using Xunit;

namespace Orleans.Transactions.Tests
{
    [TestCategory("BVT"), TestCategory("Transactions")]
    public class TocFaultTransactionMemoryTests : TocFaultTransactionTestRunner, IClassFixture<MemoryTransactionsFixture>
    {
        public TocFaultTransactionMemoryTests(MemoryTransactionsFixture fixture, ITestOutputHelper output)
            : base(fixture.GrainFactory, output)
        {
        }
    }
}
