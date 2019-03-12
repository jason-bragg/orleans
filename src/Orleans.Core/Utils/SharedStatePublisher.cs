using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public class SharedStatePublisher<T> : IStatePublisher<T>
    {
        private SharedState currentState;

        protected IAsyncLinkedListNode<T> Current => this.currentState;

        public SharedStatePublisher(T initialState)
        {
            this.currentState = new SharedState(initialState);
        }

        public void Publish(T state)
        {
            var newSateChange = new SharedState(state);
            SharedState last = Interlocked.Exchange(ref this.currentState, newSateChange);
            last.NextCompletion.TrySetResult(newSateChange);
        }

        private class SharedState : IAsyncLinkedListNode<T>
        {
            public SharedState(T state)
            {
                this.Value = state;
                this.NextCompletion = new TaskCompletionSource<IAsyncLinkedListNode<T>>();
            }

            public T Value { get; }
            public Task<IAsyncLinkedListNode<T>> NextAsync => this.NextCompletion.Task;
            public TaskCompletionSource<IAsyncLinkedListNode<T>> NextCompletion { get; }

            public void Dispose() {}
        }
    }
}
