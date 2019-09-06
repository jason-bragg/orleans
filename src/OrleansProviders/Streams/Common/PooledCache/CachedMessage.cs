
using System;

namespace Orleans.Providers.Streams.Common
{
    /// <summary>
    /// This is a tightly packed cached structure containing a queue message.
    /// It should only contain value types.
    /// </summary>
    public struct CachedMessage
    {
        /// <summary>
        /// location of sequence token in segment
        /// </summary>
        public (int Offset, int Count) SequenceTokenWindow;
        /// <summary>
        /// location of streamId in segment
        /// </summary>
        public (int Offset, int Count) StreamIdWindow;
        /// <summary>
        /// location of payload in segment
        /// </summary>
        public (int Offset, int Count) PayloadWindow;
        /// <summary>
        /// Time event was written to EventHub
        /// </summary>
        public DateTime EnqueueTimeUtc;
        /// <summary>
        /// Time event was read from EventHub into this cache
        /// </summary>
        public DateTime DequeueTimeUtc;
        /// <summary>
        /// Segment containing the serialized event data
        /// </summary>
        public ArraySegment<byte> Segment;
    }

    public static class CachedMessageExtensions
    {
        public static ReadOnlySpan<byte> SequenceToken(this ref CachedMessage cachedMessage)
            => cachedMessage.Segment.AsSpan(cachedMessage.SequenceTokenWindow.Offset, cachedMessage.SequenceTokenWindow.Count);

        public static ReadOnlySpan<byte> StreamId(this ref CachedMessage cachedMessage)
            => cachedMessage.Segment.AsSpan(cachedMessage.StreamIdWindow.Offset, cachedMessage.StreamIdWindow.Count);

        public static int Compare(this ref CachedMessage cachedMessage, in ArraySegment<byte> sequenceToken)
            => cachedMessage.SequenceToken().SequenceCompareTo(sequenceToken);

        public static bool CompareStreamId(this ref CachedMessage cachedMessage, in ArraySegment<byte> streamIdentity)
            => cachedMessage.StreamId().SequenceEqual(streamIdentity);
    }
}
