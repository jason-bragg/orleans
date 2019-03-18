using System;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public abstract class ChangeFeed<TValue> : IDisposable
    {
        public TValue Value { get; }
        public Task<ChangeFeed<TValue>> Next => this.NextCompletion.Task;

        protected TaskCompletionSource<ChangeFeed<TValue>> NextCompletion { get; }

        protected ChangeFeed(TValue state)
        {
            this.Value = state;
            this.NextCompletion = new TaskCompletionSource<ChangeFeed<TValue>>();
        }

        public virtual void Dispose()
        {
        }
    }
}
