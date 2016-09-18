using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    [Factory(FactoryAttribute.FactoryTypes.ClientObject)]
    public interface ITransactionManagerProxy : IGrain
    {
        Task<CommitTransactionsResponse> CommitTransactions(List<TransactionInfo> transactions);
        Task<StartTransactionsResponse> StartTransactions(List<TimeSpan> timeouts);

    }

    [Serializable]
    public abstract class TransactionManagerResponse
    {
        public long ReadOnlyTransactionId { get; set; }
        public long AbortLowerBound { get; set; }
    }

    [Serializable]
    public struct CommitResult
    {
        public bool Success { get; set; }

        public OrleansTransactionAbortedException AbortingException { get; set; }
    }

    [Serializable]
    public class CommitTransactionsResponse : TransactionManagerResponse
    {
        public List<CommitResult> CommitResult { get; set; }
    }

    [Serializable]
    public class StartTransactionsResponse : TransactionManagerResponse
    {
        public List<long> TransactionId { get; set; }
    }
}
