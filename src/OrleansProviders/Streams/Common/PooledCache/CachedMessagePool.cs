
using System;

namespace Orleans.Providers.Streams.Common
{
    /// <summary>
    /// Pool of tightly packed cached messages that are kept in large blocks to reduce GC pressure.
    /// </summary>
    /// <typeparam name="TCachedMessage">Tightly packed structure.  Struct should contain only value types.</typeparam>
    internal class CachedMessagePool<TCachedMessage>
        where TCachedMessage : struct
    {
        private readonly IObjectPool<CachedMessageBlock<TCachedMessage>> messagePool;
        private CachedMessageBlock<TCachedMessage> currentMessageBlock;

        /// <summary>
        /// Allocates a pool of cached message blocks.
        /// </summary>
        /// <param name="cacheDataAdapter"></param>
        public CachedMessagePool(ICacheDataAdapter<TCachedMessage> cacheDataAdapter)
        {
            messagePool = new ObjectPool<CachedMessageBlock<TCachedMessage>>(
                () => new CachedMessageBlock<TCachedMessage>());
        }

        /// <summary>
        /// Allocates a message in a block and returns the block the message is in.
        /// </summary>
        /// <returns></returns>
        public CachedMessageBlock<TCachedMessage> AllocateMessage(TCachedMessage message)
        {
            CachedMessageBlock<TCachedMessage> returnBlock = currentMessageBlock ?? (currentMessageBlock = messagePool.Allocate());
            returnBlock.Add(message);

            // blocks at capacity are eligable for purge, so we don't want to be holding on to them.
            if (!currentMessageBlock.HasCapacity)
            {
                currentMessageBlock = messagePool.Allocate();
            }

            return returnBlock;
        }
    }
}
