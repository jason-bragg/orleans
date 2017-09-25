using System;

namespace Orleans.Transactions.Abstractions
{
    [Serializable]
    public class CommitRecord
    {
        public long TransactionId { get; set; }
        public long LSN { get; set; }
    }
}
