using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public class DecoratingRecoverableStreamProcessorBase<TState, TEvent> : IRecoverableStreamProcessor<TState, TEvent>
    {
        public DecoratingRecoverableStreamProcessorBase(IRecoverableStreamProcessor<TState, TEvent> innerProcessor)
        {
            if (innerProcessor == null) { throw new ArgumentNullException(nameof(innerProcessor)); }

            this.InnerProcessor = innerProcessor;
        }

        protected IRecoverableStreamProcessor<TState, TEvent> InnerProcessor { get; }

        public virtual Task OnSetup(TState state, StreamSequenceToken token) => this.InnerProcessor.OnSetup(state, token);

        public virtual Task OnActiveStream(TState state, StreamSequenceToken token) => this.InnerProcessor.OnActiveStream(state, token);

        public virtual Task<bool> OnEvent(TState state, StreamSequenceToken token, TEvent @event) => this.InnerProcessor.OnEvent(state, token, @event);

        public virtual Task OnInactiveStream(TState state, StreamSequenceToken token) => this.InnerProcessor.OnInactiveStream(state, token);

        public virtual Task OnCleanup(TState state, StreamSequenceToken token) => this.InnerProcessor.OnCleanup(state, token);

        public virtual Task OnSave(TState state, StreamSequenceToken token) => this.InnerProcessor.OnSave(state, token);

        public virtual Task OnFastForward(TState state, StreamSequenceToken token) => this.InnerProcessor.OnFastForward(state, token);

        public virtual Task OnRecovery(TState state, StreamSequenceToken token) => this.InnerProcessor.OnRecovery(state, token);

        public virtual Task<bool> OnError(TState state, StreamSequenceToken token, Exception exception,
            object errorArgs) => this.InnerProcessor.OnError(state, token, exception, errorArgs);
    }
}