using System;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    /// <summary>
    /// The Transaction Agent it is used by the silo and activations to
    /// interact with the transactions system.
    /// </summary>
    /// <remarks>
    /// There is one Transaction Agent per silo.
    /// TODO: does this belong in Runtime instead?
    /// </remarks>
    public interface ITransactionAgent
    {
        Task<ITransactionInfo> StartTransaction(bool readOnly, TimeSpan timeout);

        Task<TransactionalStatus> ResolveTransaction(ITransactionInfo transactionInfo, bool abort);
    }
}
