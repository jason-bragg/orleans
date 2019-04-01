
namespace Orleans.Streams
{
    public class RecoverableStream<TState> : IRecoverableStream<TState>
    {
        private readonly IStreamProvider streamProvider;

        public RecoverableStream(IStreamProvider streamProvider)
        {
            this.streamProvider = streamProvider;
        }

        public IStreamIdentity StreamId => throw new System.NotImplementedException();

        public TState State => throw new System.NotImplementedException();

        public void Attach<TEvent>(IRecoverableStreamProcessor<TEvent, TState> streamingApp, IRecoverableStreamStorage<TState> storage)
        {
            throw new System.NotImplementedException();
        }
    }
}
