using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace Orleans.Transactions
{
    public interface ITransactionManagerGrain : ITransactionManagerService, IGrainWithIntegerKey
    {
    }

    [Reentrant]
    public class TransactionManagerGrain : Grain, ITransactionManagerGrain
    {
        private readonly ITransactionManager transactionManager;
        private readonly ILogger<TransactionManagerGrain> logger;
        private readonly ITransactionManagerService transactionManagerService;

        public TransactionManagerGrain(ITransactionManager transactionManager, ILoggerFactory loggerFactory)
        {
            this.transactionManager = transactionManager;
            this.logger = loggerFactory.CreateLogger<TransactionManagerGrain>();
            this.transactionManagerService = new TransactionManagerService(transactionManager);
        }

        public override async Task OnActivateAsync()
        {
            await transactionManager.StartAsync();
        }

        public override async Task OnDeactivateAsync()
        {
            await transactionManager.StopAsync();
        }

        public async Task<StartTransactionsResponse> StartTransactions(Immutable<List<TimeSpan>> timeouts)
        {
            this.logger.LogInformation("Starting {0} transactions.", timeouts.Value.Count);
            Stopwatch sw = Stopwatch.StartNew();
            StartTransactionsResponse response = await this.transactionManagerService.StartTransactions(timeouts);
            sw.Stop();
            this.logger.LogInformation("Starting {0} transactions toke {1} ticks.", timeouts.Value.Count, sw.ElapsedTicks);
            return response;
        }

        public async Task<CommitTransactionsResponse> CommitTransactions(Immutable<List<TransactionInfo>> transactions, Immutable<HashSet<long>> queries)
        {
            this.logger.LogInformation("Commiting {0} new transactions.  Querying {1}.", transactions.Value.Count, queries.Value.Count);
            Stopwatch sw = Stopwatch.StartNew();
            CommitTransactionsResponse response = await this.transactionManagerService.CommitTransactions(transactions, queries);
            sw.Stop();
            this.logger.LogInformation("Commiting transactions took {0} ticks and resolved {1} transactions.", sw.ElapsedTicks, response.CommitResult.Count);
            return response;
        }

        public Task AbortTransaction(long transactionId, OrleansTransactionAbortedException reason)
        {
            return this.transactionManagerService.AbortTransaction(transactionId, reason);
        }
    }
}
