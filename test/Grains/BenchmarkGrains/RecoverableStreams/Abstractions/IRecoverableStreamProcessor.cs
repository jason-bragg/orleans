using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public interface IRecoverableStreamProcessor<TState, TEvent>
    {
        Task OnSetup(TState state, StreamSequenceToken token);

        // Just loaded. No saving allowed.
        // This ONLY happens when we are starting a new stream or resuming a previously inactive stream. If we're resuming in the middle of a stream (deployment, silo crash, etc.), this will not be fired.
        Task OnActiveStream(TState state, StreamSequenceToken token);

        // TODO: Do I really need the token or should I just trust you?
        // Tell me if you want me to save.
        Task<bool> OnEvent(TState state, StreamSequenceToken token, TEvent @event);

        // Just detected. I'm going to save. Make any changes you want before the save.
        Task OnInactiveStream(TState state, StreamSequenceToken token);

        Task OnCleanup(TState state, StreamSequenceToken token);

        // FYI, just saved
        Task OnPersisted(TState state, StreamSequenceToken token, bool fastForward);

        // FYI, just fast forwarded. Here's the new state.
        Task OnFastForward(TState state, StreamSequenceToken token);

        // FYI, just recovered. Here's the new state we're starting with.
        Task OnRecovery(TState state, StreamSequenceToken token);

        // Orleans OnError, OnPoisonEvent, etc.
        Task<bool> OnError(TState state, StreamSequenceToken token, Exception exception, object errorArgs);
    }
}