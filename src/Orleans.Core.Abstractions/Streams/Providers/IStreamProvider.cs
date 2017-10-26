using System;

namespace Orleans.Streams
{
    public interface IStreamProvider
    {
        IAsyncStream<T> GetStream<T>(Guid streamId, string streamNamespace);
        /// <summary>
        /// Determines whether this is a rewindable provider - supports creating rewindable streams 
        /// (streams that allow subscribing from previous point in time).
        /// </summary>
        /// <returns>True if this is a rewindable provider, false otherwise.</returns>
        bool IsRewindable { get; }
    }
}