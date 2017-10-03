using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.Tests
{
    public abstract class GoldenPathTransactionManagerTestRunner : IDisposable
    {
        private readonly ITestOutputHelper output;

        protected abstract ITransactionManager TransactionManager { get; }

        protected GoldenPathTransactionManagerTestRunner(ITestOutputHelper output)
        {
            this.output = output;
        }

        [SkippableFact]
        public async Task StartCommitTransaction()
        {
            var id = await TransactionManager.StartTransactions(new List<TimeSpan>( new TimeSpan[]{ TimeSpan.FromMinutes(1) }));
            var info = new TransactionInfo(id[0]);
            TransactionManager.CommitTransaction(info);
            await WaitForTransactionCommit(id[0]);
        }

        [SkippableFact]
        public async Task TransactionTimeout()
        {
            var id = await TransactionManager.StartTransactions(new List<TimeSpan>(new TimeSpan[] { TimeSpan.FromMilliseconds(1) }));
            var info = new TransactionInfo(id[0]);
            await Task.Delay(3000);

            TransactionManager.CommitTransaction(info);
            await Assert.ThrowsAsync<OrleansTransactionTimeoutException>(() => WaitForTransactionCommit(id[0]));
        }

        [SkippableFact]
        public async Task DependentTransaction()
        {
            var ids = await TransactionManager.StartTransactions(new List<TimeSpan>(new TimeSpan[] { TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2) }));

            var info = new TransactionInfo(ids[0]);
            TransactionManager.CommitTransaction(info);
            await WaitForTransactionCommit(ids[0]);

            var info2 = new TransactionInfo(ids[1]);
            info2.DependentTransactions.Add(ids[0]);
            TransactionManager.CommitTransaction(info2);
            await WaitForTransactionCommit(ids[1]);
        }

        [SkippableFact]
        public async Task OutOfOrderCommitTransaction()
        {
            var ids = await TransactionManager.StartTransactions(new List<TimeSpan>(new TimeSpan[] { TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2) }));

            var info2 = new TransactionInfo(ids[1]);
            info2.DependentTransactions.Add(ids[0]);

            TransactionManager.CommitTransaction(info2);
            OrleansTransactionAbortedException e;
            Assert.True(TransactionManager.GetTransactionStatus(ids[1], out e) == TransactionStatus.InProgress);

            var info = new TransactionInfo(ids[0]);
            TransactionManager.CommitTransaction(info);

            await WaitForTransactionCommit(ids[1]);
        }

        [SkippableFact]
        public async Task CascadingAbortTransaction()
        {
            var ids = await TransactionManager.StartTransactions(new List<TimeSpan>(new TimeSpan[] { TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2) }));

            var info2 = new TransactionInfo(ids[1]);
            info2.DependentTransactions.Add(ids[0]);

            TransactionManager.CommitTransaction(info2);
            OrleansTransactionAbortedException abort;
            Assert.True(TransactionManager.GetTransactionStatus(ids[1], out abort) == TransactionStatus.InProgress);

            TransactionManager.AbortTransaction(ids[0], new OrleansTransactionAbortedException(ids[0]));

            var e = await Assert.ThrowsAsync<OrleansCascadingAbortException>(() => WaitForTransactionCommit(ids[1]));
            Assert.True(e.TransactionId == ids[1]);
            Assert.True(e.DependentTransactionId == ids[0]);
        }

        private async Task WaitForTransactionCommit(long transactionId)
        {
            TimeSpan timeout = TimeSpan.FromSeconds(15);

            var endTime = DateTime.UtcNow + timeout;
            while (DateTime.UtcNow < endTime)
            {
                OrleansTransactionAbortedException e;
                var result = TransactionManager.GetTransactionStatus(transactionId, out e);
                switch (result)
                {
                    case TransactionStatus.Committed:
                        return;
                    case TransactionStatus.Aborted:
                        throw e;
                    case TransactionStatus.Unknown:
                        throw new OrleansTransactionInDoubtException(transactionId);
                    default:
                        Assert.True(result == TransactionStatus.InProgress);
                        await Task.Delay(100);
                        break;
                }
            }

            throw new TimeoutException("Timed out waiting for the transaction to complete");
        }

        public void Dispose()
        {
            (this.TransactionManager as IDisposable)?.Dispose();
        }
    }
}
