using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public class TMProxyDirectoryGrain : Grain, ITMProxyDirectoryGrain
    {
        private ITransactionManagerProxy reference;

        public Task SetReference(ITransactionManagerProxy reference)
        {
            this.reference = reference;
            return TaskDone.Done;
        }

        public Task<ITransactionManagerProxy> GetReference()
        {
            return Task.FromResult<ITransactionManagerProxy>(reference);
        }
    }
}
