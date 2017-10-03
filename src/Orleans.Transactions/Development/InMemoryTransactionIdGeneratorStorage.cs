using System;
using System.Threading.Tasks;
using Orleans.Transactions.Abstractions;
using Orleans.Runtime;
using System.Linq;

namespace Orleans.Transactions.Development
{
    class InMemoryTransactionIdGeneratorStorage : ITransactionIdGeneratorStorage
    {
        private readonly Logger logger;
        private long currentId = 1;

        public InMemoryTransactionIdGeneratorStorage(Factory<string,Logger> loggerFactory)
        {
            this.logger = loggerFactory(nameof(InMemoryTransactionIdGeneratorStorage));
        }
        public Task<long[]> AllocateSequentialIds(Guid requestorId, int allocationBatchSize)
        {
            logger.Info($"Allocating {allocationBatchSize} transaction Ids, {this.currentId} - {this.currentId + allocationBatchSize}");
            return Task.FromResult(Enumerable.Range(0,allocationBatchSize).Select(i => this.currentId++).ToArray());
        }
    }
}
