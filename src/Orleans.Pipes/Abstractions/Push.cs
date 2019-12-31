using System;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Pipes.Abstractions
{
    public struct SequentialData<TData>
    {
        public SequentialData(TData data, byte[] position)
        {
            this.Data = data ?? throw new ArgumentNullException(nameof(data));
            this.Position = position ?? throw new ArgumentNullException(nameof(position));
        }

        public TData Data { get; }
        public byte[] Position { get; }
    }

    public struct SendResult
    {
        public SendResult(byte[] consumed, byte[] read)
        {
            this.Consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));
            this.Read = read ?? throw new ArgumentNullException(nameof(read));
        }

        public byte[] Consumed { get; }
        public byte[] Read { get; }
    }

    public struct FlushResult
    {
        public FlushResult(byte[] consumed)
        {
            this.Consumed = consumed ?? throw new ArgumentNullException(nameof(consumed));
        }

        public byte[] Consumed { get; }
    }

    public interface IPushPipeOut<TKey, TData>
    {
        TKey Id { get; }
        Task<SendResult> Send(SequentialData<TData>[] data, CancellationToken cancellationToken = default);
        Task<FlushResult> Flush(byte[] position, CancellationToken cancellationToken = default);
        Task Close(Exception ex = null, CancellationToken cancellationToken = default);
    }

    public interface IPushPipeGrainConnector
    {
        IPushPipeOut<TKey, TData> Create<TKey, TData>(TKey key, GrainReference grainRef);
    }

    public interface IPushPipeIn<TKey, TData>
    {
        Task<SendResult> OnReceive(SequentialData<TData>[] data, CancellationToken cancellationToken = default);
        Task<FlushResult> OnFlush(byte[] position, CancellationToken cancellationToken = default);
        Task OnClose(Exception ex = null, CancellationToken cancellationToken = default);
    }

    public interface IPushPipeListener
    {
        IDisposable Listen<TKey, TData>(Func<TKey, IPushPipeIn<TKey, TData>> inFactory, Action<TKey, IDisposable> inRegistery);
        IDisposable Listen<TKey, TData>(TKey key, IPushPipeIn<TKey, TData> pushPipeIn);
    }
}
