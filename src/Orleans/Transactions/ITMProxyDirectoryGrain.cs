using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public interface ITMProxyDirectoryGrain : IGrainWithIntegerKey
    {
        Task<ITransactionManagerProxy> GetReference();
        Task SetReference(ITransactionManagerProxy reference);
    }
}
