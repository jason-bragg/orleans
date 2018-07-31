
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions.Abstractions
{
    public interface ITransactionParticipant : IEquatable<ITransactionParticipant>
    {
        Task Prepare(Guid transactionId, AccessCounter accessCount,
            DateTime timeStamp, ITransactionManager transactionManager);

        Task Commit(Guid transactionId, DateTime timeStamp, TransactionalStatus status);
    }

    public interface ITransactionManager
    {
        Task<TransactionalStatus> Resolve(Guid transactionId, AccessCounter accessCount, DateTime timeStamp,
            IList<ITransactionParticipant> writeParticipants, int totalParticipants);

        Task Report(Guid transactionId, DateTime timeStamp, ITransactionParticipant participant, TransactionalStatus status);
    }

    /// <summary>
    /// Counts read and write accesses on a transaction participant.
    /// </summary>
    [Serializable]
    public struct AccessCounter
    {
        public int Reads;
        public int Writes;

        public static AccessCounter operator +(AccessCounter c1, AccessCounter c2)
        {
            return new AccessCounter { Reads = c1.Reads + c2.Reads, Writes = c1.Writes + c2.Writes };
        }
    }
}

  
   

