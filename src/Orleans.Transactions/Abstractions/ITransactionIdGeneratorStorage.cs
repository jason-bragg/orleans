using System;
using System.Threading.Tasks;

namespace Orleans.Transactions.Abstractions
{
    public interface ITransactionIdGeneratorStorage
    {
        Task<long[]> AllocateSequentialIds(Guid requestorId, int allocationBatchSize);
    }
}
