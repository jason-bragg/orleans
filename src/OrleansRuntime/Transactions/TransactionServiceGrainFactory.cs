
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class TransactionServiceGrainFactory : ITransactionServiceFactory
    {
        private readonly IGrainFactory grainFactory;

        public TransactionServiceGrainFactory(IGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory;
        }

        public Task<ITransactionStartService> GetTransactionStartService()
        {
            return Task.FromResult<ITransactionStartService>(new TransactionManagerServiceWrapper(this.grainFactory.GetGrain<ITransactionManagerGrain>(0)));
        }

        public Task<ITransactionCommitService> GetTransactionCommitService()
        {
            return Task.FromResult<ITransactionCommitService>(new TransactionManagerServiceWrapper(this.grainFactory.GetGrain<ITransactionManagerGrain>(0)));
        }

        private class TransactionManagerServiceWrapper : ITransactionManagerService
        {
            private readonly ITransactionManagerGrain transactionManagerGrain;

            public TransactionManagerServiceWrapper(ITransactionManagerGrain transactionManagerGrain)
            {
                this.transactionManagerGrain = transactionManagerGrain;
            }

            public Task<StartTransactionsResponse> StartTransactions(List<TimeSpan> timeouts)
            {
                return transactionManagerGrain.StartTransactions(timeouts);
            }

            public Task<CommitTransactionsResponse> CommitTransactions(List<TransactionInfo> transactions)
            {
                return transactionManagerGrain.CommitTransactions(transactions);
            }
        }
    }
}
