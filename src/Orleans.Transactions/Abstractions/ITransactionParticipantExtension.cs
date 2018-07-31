using System;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.Transactions.Abstractions
{
    /// <summary>
    /// This is a grain extension interface that allows a grain to be a participant in a transaction.
    /// For documentation of the methods, see <see cref="ITransactionParticipant"/>.
    /// </summary>
    public interface ITransactionParticipantExtension : IGrainExtension
    {
        [AlwaysInterleave]
        [Transaction(TransactionOption.Suppress)]
        [OneWay]
        Task Prepare(string resourceId, Guid transactionId, AccessCounter accessCount,
            DateTime timeStamp, ITransactionManager transactionManager);

        [AlwaysInterleave]
        [Transaction(TransactionOption.Suppress)]
        Task Commit(string resourceId, Guid transactionId, DateTime timeStamp, TransactionalStatus status);
    }
}
