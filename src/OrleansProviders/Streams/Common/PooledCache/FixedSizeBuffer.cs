
using System;

namespace Orleans.Providers.Streams.Common
{
    /// <summary>
    /// Manages a contiguous block of memory.
    /// Calls purge action with itself as the purge request when it's signaled to purge.
    /// </summary>
    public class FixedSizeBuffer : PooledResource<FixedSizeBuffer>
    {
        private readonly byte[] buffer;
        private int count;
        private readonly int blockSize;

        /// <summary>
        /// Unique identifier of this buffer
        /// </summary>
        public object Id => buffer;

        /// <summary>
        /// Manages access to a fixed size byte buffer.
        /// </summary>
        /// <param name="blockSize"></param>
        public FixedSizeBuffer(int blockSize)
        {
            if (blockSize < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(blockSize), "blockSize must be positive value.");
            }
            count = 0;
            this.blockSize = blockSize;
            buffer = new byte[blockSize];
        }

        /// <summary>
        /// Try to get a segment with a buffer of the specified size from this block.
        /// Fail if there is not enough space available
        /// </summary>
        /// <param name="size"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryGetSegment(int size, out ArraySegment<byte> value)
        {
            value = default(ArraySegment<byte>);
            if (size > blockSize - count)
            {
                return false;
            }
            value = new ArraySegment<byte>(buffer, count, size);
            count += size;
            return true;
        }

        /// <inheritdoc />
        public override void OnResetState()
        {
            count = 0;
        }
    }
}
