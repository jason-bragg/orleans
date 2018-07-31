using System;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class DisabledTransactionAgent : ITransactionAgent
    {
        public Task<ITransactionInfo> StartTransaction(bool readOnly, TimeSpan timeout)
        {
            throw new OrleansStartTransactionFailedException(new OrleansTransactionsDisabledException());
        }

        public Task<TransactionalStatus> ResolveTransaction(ITransactionInfo transactionInfo, bool abort)
        {
            throw new OrleansTransactionsDisabledException();
        }
    }
}
