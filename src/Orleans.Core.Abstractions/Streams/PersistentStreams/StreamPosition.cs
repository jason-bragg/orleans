
using System;

namespace Orleans.Streams
{
    /// <summary>
    /// Stream position uniquely identifies the position of an event in a stream.
    /// If acquiring a stream position for a batch of events, the stream position will be of the first event in the batch.
    /// </summary>
    public class StreamPosition
    {
        /// <summary>
        /// Stream position consists of the stream identity and the sequence token
        /// </summary>
        /// <param name="streamIdentity"></param>
        /// <param name="sequenceToken"></param>
        public StreamPosition(in ArraySegment<byte> streamIdentity, in ArraySegment<byte> sequenceToken)
        {
            if (streamIdentity == null)
            {
                throw new ArgumentNullException("streamIdentity");
            }
            if (sequenceToken == null)
            {
                throw new ArgumentNullException("sequenceToken");
            }
            this.StreamIdentity = streamIdentity;
            this.SequenceToken = sequenceToken;
        }
        /// <summary>
        /// Identity of the stream
        /// </summary>
        public ArraySegment<byte> StreamIdentity { get; }

        /// <summary>
        /// Position in the stream
        /// </summary>
        public ArraySegment<byte> SequenceToken { get; }
    }
}
