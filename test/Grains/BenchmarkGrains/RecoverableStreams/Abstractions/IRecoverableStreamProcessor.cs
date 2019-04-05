using System.Threading.Tasks;

namespace Orleans.Streams
{
    public interface IRecoverableStreamProcessor<TState, TEvent>
    {
        Task OnActiveStream(TState state);
        Task<bool> OnEvent(TEvent evt, StreamSequenceToken token, TState state);
        Task OnInactiveStream(TState state);
    }
}