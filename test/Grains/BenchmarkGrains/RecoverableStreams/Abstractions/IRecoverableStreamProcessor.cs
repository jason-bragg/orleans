using System.Threading.Tasks;

namespace Orleans.Streams
{
    public interface IRecoverableStreamProcessor<TState, TEvent>
    {
        // Just loaded. No saving allowed.
        Task OnActiveStream(TState state);

        // Tell me if you want me to save.
        Task<bool> OnEvent(TEvent evt, StreamSequenceToken token, TState state);

        // Just detected. I'm going to save. Make any changes you want before the save.
        Task OnInactiveStream(TState state);

        // FYI, just fast forwarded. Here's the new state.
        Task OnFastForward(TState state);

        // FYI, just recovered. Here's the new state we're starting with.
        Task OnRecovery(TState state);
    }
}