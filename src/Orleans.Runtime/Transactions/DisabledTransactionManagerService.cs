using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Transactions;
using Orleans.Concurrency;

namespace Orleans.Transactions
{
    internal class DisabledTransactionManagerService : ITransactionManagerService
    {
        public Task AbortTransaction(long transactionId, OrleansTransactionAbortedException reason)
        {
            throw new OrleansTransactionsDisabledException();
        }

        public Task<CommitTransactionsResponse> CommitTransactions(Immutable<List<TransactionInfo>> transactions, Immutable<HashSet<long>> queries)
        {
            throw new OrleansTransactionsDisabledException();
        }

        public Task<StartTransactionsResponse> StartTransactions(Immutable<List<TimeSpan>> timeouts)
        {
            throw new OrleansTransactionsDisabledException();
        }
    }
}
