using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public class SharedStatePublisher<T> : IStatePublisher<T>
    {
        private SharedState currentState;

        protected ChangeFeed<T> Current => this.currentState;

        public SharedStatePublisher(T initialState)
        {
            this.currentState = new SharedState(initialState);
        }

        public void Publish(T state)
        {
            var newSateChange = new SharedState(state);
            SharedState last = Interlocked.Exchange(ref this.currentState, newSateChange);
            last.Completion.TrySetResult(newSateChange);
        }

        private class SharedState : ChangeFeed<T>
        {
            public SharedState(T state) : base(state) { }
            public TaskCompletionSource<ChangeFeed<T>> Completion => base.NextCompletion;
        }
    }
}
