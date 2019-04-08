using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public class RecoverableStreamProcessorBase<TState, TEvent> : IRecoverableStreamProcessor<TState, TEvent>
    {
        public virtual Task OnSetup(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task OnActiveStream(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task<bool> OnEvent(TState state, StreamSequenceToken token, TEvent @event) => Task.FromResult(false);

        public virtual Task OnInactiveStream(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task OnCleanup(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task OnSave(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task OnFastForward(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task OnRecovery(TState state, StreamSequenceToken token) => Task.CompletedTask;

        public virtual Task<bool> OnError(TState state, StreamSequenceToken token, Exception exception,
            object errorArgs) => Task.FromResult(false);
    }
}