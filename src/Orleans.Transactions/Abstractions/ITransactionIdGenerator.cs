using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Orleans.Transactions.Abstractions
{
    public interface ITransactionIdGenerator
    {
        Task<long[]> GenerateTransactionIds(int count);
    }

    [Serializable]
    public class TransactionIdGeneratorServiceNotAvailableException : OrleansTransactionException
    {
        public TransactionIdGeneratorServiceNotAvailableException() : base("TransactionId Generator service not available"){}

        public TransactionIdGeneratorServiceNotAvailableException(string message) : base(message) { }

        public TransactionIdGeneratorServiceNotAvailableException(string message, Exception innerException) : base(message, innerException) { }

        public TransactionIdGeneratorServiceNotAvailableException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
