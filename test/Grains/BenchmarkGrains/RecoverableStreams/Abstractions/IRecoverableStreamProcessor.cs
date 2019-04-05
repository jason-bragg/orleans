using System.Threading.Tasks;

namespace Orleans.Streams
{
    public interface IRecoverableStreamProcessor<TState, TEvent>
    {
        Task OnSetup(TState state);

        // Just loaded. No saving allowed.
        // This ONLY happens when we are starting a new stream or resuming a previously inactive stream. If we're resuming in the middle of a stream (deployment, silo crash, etc.), this will not be fired.
        Task OnActiveStream(TState state);

        // Tell me if you want me to save.
        Task<bool> OnEvent(TEvent @event, StreamSequenceToken token, TState state);

        // Just detected. I'm going to save. Make any changes you want before the save.
        Task OnInactiveStream(TState state);

        Task OnCleanup(TState state);

        // FYI, just fast forwarded. Here's the new state.
        Task OnFastForward(TState state);

        // FYI, just recovered. Here's the new state we're starting with.
        Task OnRecovery(TState state);
    }
}