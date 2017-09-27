
using System.Threading.Tasks;
using System.Threading;
using Microsoft.Extensions.Options;
using Xunit.Abstractions;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions.Development;
using Orleans.LeaseProviders;
using Orleans.Runtime;
using Orleans.TestingHost.Utils;
using System.Linq;
using System;

namespace Orleans.Transactions.Tests
{
    [TestCategory("BVT"), TestCategory("Transactions")]
    public class GoldenPathTransactionManagerMemoryTests : GoldenPathTransactionManagerTestRunner
    {
        public GoldenPathTransactionManagerMemoryTests(ITestOutputHelper output)
            : base(output)
        {
            ILeaseProvider leaseProvider = new NoOpILeaseProvider();
            Factory<string, Logger> loggerFactory = (s) => new NoOpTestLogger();
            ITransactionIdGeneratorStorage storage = new InMemoryTransactionIdGeneratorStorage(loggerFactory);
            Factory<Task<ITransactionIdGenerator>> idGeneratorFactory = async () =>
            {
                var generator = new TransactionIdGenerator(new TestConfig<TransactionIdGeneratorConfig>(), leaseProvider, storage);
                await generator.Initialize();
                return generator;
            };
            ITransactionManager tm = new TransactionManager(new TestConfig<TransactionsConfiguration>(), new TransactionLog(new InMemoryTransactionLogStorage()), idGeneratorFactory, loggerFactory);
            tm.StartAsync().GetAwaiter().GetResult();
            TransactionManager = tm;
        }

        protected override ITransactionManager TransactionManager { get; }

        private class TestConfig<T> : IOptions<T>
            where T : class, new()
        {
            public T Value => new T();
        }

        // Placeholder lease provider. Recoverablity testing will require real one
        private class NoOpILeaseProvider : ILeaseProvider
        {
            public Task<AcquireLeaseResult[]> Acquire(string category, LeaseRequest[] leaseRequests)
            {
                return Task.FromResult(leaseRequests.Select(request => new AcquireLeaseResult(new AcquiredLease(request.ResourceKey, request.Duration, string.Empty, DateTime.UtcNow), ResponseCode.OK, null)).ToArray());
            }

            public Task Release(string category, AcquiredLease[] aquiredLeases)
            {
                return Task.CompletedTask;
            }

            public Task<AcquireLeaseResult[]> Renew(string category, AcquiredLease[] aquiredLeases)
            {
                return Task.FromResult(aquiredLeases.Select(lease => new AcquireLeaseResult(new AcquiredLease(lease.ResourceKey, lease.Duration, string.Empty, DateTime.UtcNow), ResponseCode.OK, null)).ToArray());
            }
        }
    }
}
