using Orleans.Core;

namespace Orleans.Streams
{
    public interface IRecoverableStream<TState, TEvent>
    {
        IStreamIdentity StreamId { get; }
        TState State { get; }

        void Attach(IRecoverableStreamProcessor<TState, TEvent> processor,
            IAdvancedStorage<RecoverableStreamState<TState>> storage,
            IRecoverableStreamStoragePolicy storagePolicy);
    }
}
