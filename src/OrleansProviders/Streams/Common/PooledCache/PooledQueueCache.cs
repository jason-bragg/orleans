
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Common
{
    /// <summary>
    /// The PooledQueueCache is a cache that is intended to serve as a message cache in an IQueueCache.
    /// It is capable of storing large numbers of messages (gigs worth of messages) for extended periods
    ///   of time (minutes to indefinite), while incurring a minimal performance hit due to garbage collection.
    /// This pooled cache allocates memory and never releases it. It keeps freed resources available in pools 
    ///   that remain in application use through the life of the service. This means these objects go to gen2,
    ///   are compacted, and then stay there. This is relatively cheap, as the only cost they now incur is
    ///   the cost of checking to see if they should be freed in each collection cycle. Since this cache uses
    ///   small numbers of large objects with relatively simple object graphs, they are less costly to check
    ///   then large numbers of smaller objects with more complex object graphs.
    /// For performance reasons this cache is designed to more closely align with queue specific data.  This is,
    ///   in part, why, unlike the SimpleQueueCache, this cache does not implement IQueueCache.  It is intended
    ///   to be used in queue specific implementations of IQueueCache.
    /// </summary>
    public class PooledQueueCache: IPurgeObservable
    {
        // linked list of message bocks.  First is newest.
        private readonly LinkedList<CachedMessageBlock> messageBlocks;
        private readonly CachedMessagePool pool;
        private readonly ICacheDataAdapter cacheDataAdapter;
        private readonly ICacheMonitor cacheMonitor;
        private readonly PeriodicAction periodicMonitoring;

        /// <summary>
        /// Cached message most recently added
        /// </summary>
        public CachedMessage? Newest => this.IsEmpty
            ? null
            : (CachedMessage?)this.messageBlocks.First.Value.NewestMessage;

        /// <summary>
        /// Oldest message in cache
        /// </summary>
        public CachedMessage? Oldest => this.IsEmpty
            ? null
            : (CachedMessage?)this.messageBlocks.Last.Value.OldestMessage;

        /// <summary>
        /// Cached message count
        /// </summary>
        public int ItemCount { get; private set; }

        /// <summary>
        /// Pooled queue cache is a cache of message that obtains resource from a pool
        /// </summary>
        /// <param name="cacheDataAdapter"></param>
        /// <param name="cacheMonitor"></param>
        /// <param name="cacheMonitorWriteInterval">cache monitor write interval.  Only triggered for active caches.</param>
        public PooledQueueCache(ICacheDataAdapter cacheDataAdapter, ICacheMonitor cacheMonitor, TimeSpan? cacheMonitorWriteInterval)
        {
            this.cacheDataAdapter = cacheDataAdapter ?? throw new ArgumentNullException(nameof(cacheDataAdapter));
            this.ItemCount = 0;
            this.pool = new CachedMessagePool();
            this.messageBlocks = new LinkedList<CachedMessageBlock>();
            this.cacheMonitor = cacheMonitor;

            if (this.cacheMonitor != null && cacheMonitorWriteInterval.HasValue)
            {
                this.periodicMonitoring = new PeriodicAction(cacheMonitorWriteInterval.Value, this.ReportCacheMessageStatistics);
            }
        }

        /// <summary>
        /// Indicates whether the cache is empty
        /// </summary>
        public bool IsEmpty => this.messageBlocks.Count == 0 ||
                               this.messageBlocks.Count == 1 && this.messageBlocks.First.Value.IsEmpty;

        /// <summary>
        /// Acquires a cursor to enumerate through the messages in the cache at the provided sequenceToken, 
        ///   filtered on the specified stream.
        /// </summary>
        /// <param name="streamIdentity">stream identity</param>
        /// <param name="sequenceToken"></param>
        /// <returns></returns>
        public object GetCursor(in ArraySegment<byte> streamIdentity, in ArraySegment<byte> sequenceToken)
        {
            var cursor = new Cursor(streamIdentity);
            this.SetCursor(cursor, sequenceToken);
            return cursor;
        }

        private void ReportCacheMessageStatistics()
        {
            if (this.IsEmpty)
            {
                this.cacheMonitor.ReportMessageStatistics(null, null, null, this.ItemCount);
            }
            else
            {
                var newestMessage = this.Newest.Value;
                var oldestMessage = this.Oldest.Value;
                var newestMessageEnqueueTime = newestMessage.EnqueueTimeUtc;
                var oldestMessageEnqueueTime = oldestMessage.EnqueueTimeUtc;
                var oldestMessageDequeueTime = oldestMessage.DequeueTimeUtc;
                this.cacheMonitor.ReportMessageStatistics(oldestMessageEnqueueTime, oldestMessageDequeueTime, newestMessageEnqueueTime, this.ItemCount);
            }
        }

        private void SetCursor(Cursor cursor, in ArraySegment<byte> sequenceToken)
        {
            // If nothing in cache, unset token, and wait for more data.
            if (this.messageBlocks.Count == 0)
            {
                cursor.State = CursorStates.Unset;
                cursor.SequenceToken = sequenceToken;
                return;
            }

            LinkedListNode<CachedMessageBlock> newestBlock = this.messageBlocks.First;

            // if sequenceToken is null, iterate from newest message in cache
            if (sequenceToken.Array == null)
            {
                cursor.State = CursorStates.Idle;
                cursor.CurrentBlock = newestBlock;
                cursor.Index = newestBlock.Value.NewestMessageIndex;
                cursor.SequenceToken = newestBlock.Value.GetNewestSequenceToken();
                return;
            }

            // If sequenceToken is too new to be in cache, unset token, and wait for more data.
            CachedMessage newestMessage = newestBlock.Value.NewestMessage;
            if (newestMessage.Compare(sequenceToken) < 0) 
            {
                cursor.State = CursorStates.Unset;
                cursor.SequenceToken = sequenceToken;
                return;
            }

            // Check to see if sequenceToken is too old to be in cache
            CachedMessage oldestMessage = this.messageBlocks.Last.Value.OldestMessage;
            if (oldestMessage.Compare(sequenceToken) > 0)
            {
                // throw cache miss exception
                throw new QueueCacheMissException(sequenceToken,
                    this.messageBlocks.Last.Value.GetOldestSequenceToken(),
                    this.messageBlocks.First.Value.GetNewestSequenceToken());
            }

            // Find block containing sequence number, starting from the newest and working back to oldest
            LinkedListNode<CachedMessageBlock> node = this.messageBlocks.First;
            while (true)
            {
                CachedMessage oldestMessageInBlock = node.Value.OldestMessage;
                if (oldestMessageInBlock.Compare(sequenceToken) <= 0)
                {
                    break;
                }
                node = node.Next;
            }

            // return cursor from start.
            cursor.CurrentBlock = node;
            cursor.Index = node.Value.GetIndexOfFirstMessageLessThanOrEqualTo(sequenceToken);
            // if cursor has been idle, move to next message after message specified by sequenceToken  
            if(cursor.State == CursorStates.Idle)
            {
                // if there are more messages in this block, move to next message
                if (!cursor.IsNewestInBlock)
                {
                    cursor.Index++;
                }
                // if this is the newest message in this block, move to oldest message in newer block
                else if (node.Previous != null)
                {
                    cursor.CurrentBlock = node.Previous;
                    cursor.Index = cursor.CurrentBlock.Value.OldestMessageIndex;
                }
                else
                {
                    cursor.State = CursorStates.Idle;
                    return;
                }
            }
            cursor.SequenceToken = cursor.CurrentBlock.Value.GetSequenceToken(cursor.Index);
            cursor.State = CursorStates.Set;
        }

        /// <summary>
        /// Acquires the next message in the cache at the provided cursor
        /// </summary>
        /// <param name="cursorObj"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public bool TryGetNextMessage(object cursorObj, out CachedMessage message)
        {
            message = default;

            if (cursorObj == null)
            {
                throw new ArgumentNullException("cursorObj");
            }

            if (!(cursorObj is Cursor cursor))
            {
                throw new ArgumentOutOfRangeException("cursorObj", "Cursor is bad");
            }

            if (cursor.State != CursorStates.Set)
            {
                this.SetCursor(cursor, cursor.SequenceToken);
                if (cursor.State != CursorStates.Set)
                {
                    return false;
                }
            }

            // has this message been purged
            CachedMessage oldestMessage = messageBlocks.Last.Value.OldestMessage;
            if (oldestMessage.Compare(cursor.SequenceToken) > 0)
            {
                throw new QueueCacheMissException(cursor.SequenceToken,
                    this.messageBlocks.Last.Value.GetOldestSequenceToken(),
                    this.messageBlocks.First.Value.GetNewestSequenceToken());
            }

            // Iterate forward (in time) in the cache until we find a message on the stream or run out of cached messages.
            // Note that we get the message from the current cursor location, then move it forward.  This means that if we return true, the cursor
            //   will point to the next message after the one we're returning.
            while (cursor.State == CursorStates.Set)
            {
                CachedMessage currentMessage = cursor.Message;

                // Have we caught up to the newest event, if so set cursor to idle.
                if (cursor.CurrentBlock == this.messageBlocks.First && cursor.IsNewestInBlock)
                {
                    cursor.State = CursorStates.Idle;
                    cursor.SequenceToken = this.messageBlocks.First.Value.GetNewestSequenceToken();
                }
                else // move to next
                {
                    int index;
                    if (cursor.IsNewestInBlock)
                    {
                        cursor.CurrentBlock = cursor.CurrentBlock.Previous;
                        cursor.CurrentBlock.Value.TryFindFirstMessage(cursor.StreamIdentity, out index);
                    }
                    else
                    {
                        cursor.CurrentBlock.Value.TryFindNextMessage(cursor.Index + 1, cursor.StreamIdentity, out index);
                    }
                    cursor.Index = index;
                }

                // check if this message is in the cursor's stream
                if (currentMessage.CompareStreamId(cursor.StreamIdentity))
                {
                    message = currentMessage;
                    cursor.SequenceToken = cursor.CurrentBlock.Value.GetSequenceToken(cursor.Index);
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Add a list of queue message to the cache 
        /// </summary>
        /// <param name="messages"></param>
        /// <param name="dequeueTime"></param>
        /// <returns></returns>
        public void Add(List<CachedMessage> messages, in DateTime dequeueTime)
        {
            foreach (var message in messages)
            {
                this.Add(message);
            }
            this.cacheMonitor?.TrackMessagesAdded(messages.Count);
            this.periodicMonitoring?.TryAction(dequeueTime);
        }

        private void Add(in CachedMessage message)
        {
            // allocate message from pool
            CachedMessageBlock block = pool.AllocateMessage(message);

            // If new block, add message block to linked list
            if (block != this.messageBlocks.FirstOrDefault())
                this.messageBlocks.AddFirst(block.Node);
            this.ItemCount++;
        }

        /// <summary>
        /// Remove oldest message in the cache, remove oldest block too if the block is empty
        /// </summary>
        public void RemoveOldestMessage()
        {
            this.messageBlocks.Last.Value.Remove();
            this.ItemCount--;
            CachedMessageBlock lastCachedMessageBlock = this.messageBlocks.Last.Value;
            // if block is currently empty, but all capacity has been exausted, remove
            if (lastCachedMessageBlock.IsEmpty && !lastCachedMessageBlock.HasCapacity)
            {
                lastCachedMessageBlock.Dispose();
                this.messageBlocks.RemoveLast();
            }
        }

        private enum CursorStates
        {
            Unset, // Not yet set, or points to some data in the future.
            Set, // Points to a message in the cache
            Idle, // Has iterated over all relevant events in the cache and is waiting for more data on the stream.
        }

        private class Cursor
        {
            public readonly ArraySegment<byte> StreamIdentity;

            public Cursor(in ArraySegment<byte> streamIdentity)
            {
                StreamIdentity = streamIdentity;
                State = CursorStates.Unset;
            }

            public CursorStates State;

            // current sequence token
            public ArraySegment<byte> SequenceToken;

            // reference into cache
            public LinkedListNode<CachedMessageBlock> CurrentBlock;
            public int Index;

            // utilities
            public bool IsNewestInBlock => this.Index == CurrentBlock.Value.NewestMessageIndex;
            public CachedMessage Message => this.CurrentBlock.Value[Index];
        }
    }
}
