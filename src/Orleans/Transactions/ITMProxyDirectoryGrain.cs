
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public interface ITMProxyDirectoryGrain : IGrainWithIntegerKey
    {
        Task<ITransactionManagerProxy> GetReference();
        Task SetReference(ITransactionManagerProxy reference);
    }
}
