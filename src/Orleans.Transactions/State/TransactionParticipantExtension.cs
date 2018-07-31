
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions
{
    public class TransactionParticipantExtension : ITransactionParticipantExtension
    {
        private readonly Dictionary<string, ITransactionParticipant> localParticipants = new Dictionary<string, ITransactionParticipant>();

        public void Register(string resourceId, ITransactionParticipant localTransactionParticipant)
        {
            this.localParticipants.Add(resourceId, localTransactionParticipant);
        }

        public Task Prepare(string resourceId, Guid transactionId, AccessCounter accessCount,
            DateTime timeStamp, ITransactionManager transactionManager)
        {
            return localParticipants[resourceId].Prepare(transactionId, accessCount, timeStamp, transactionManager);
        }

        public Task Commit(string resourceId, Guid transactionId, DateTime timeStamp, TransactionalStatus status)
        {
            return localParticipants[resourceId].Commit(transactionId, timeStamp, status);
        }
    }
}
