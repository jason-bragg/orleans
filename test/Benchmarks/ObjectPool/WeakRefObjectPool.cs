
using System;
using System.Threading;
using System.Collections.Concurrent;
using Orleans.Providers.Streams.Common;
using Orleans;
using System.Buffers;

namespace Benchmarks.ObjectPool
{
    public struct DataBlock
    {
        private readonly object marker;

        public DataBlock(object marker, byte[] data)
        {
            this.Data = new ReadOnlySequence<byte>(data);
            this.marker = marker;
        }
        public ReadOnlySequence<byte> Data { get; }

        public override string ToString() => this.marker.ToString();

        internal bool IsValid => marker != null;
    }

    public interface IObjectPoolMonitor2 : IObjectPoolMonitor
    {
        void Report2(long chunks, long reusedChunks, long gcCount);
    }

    public class WeakRefObjectPool
    {
        private readonly int blocksize;
        private readonly IObjectPoolMonitor2 monitor;
        private readonly PeriodicAction periodicMonitoring;

        private long totalObjects;
        private long reusedChunks;
        private long gcCount;
        private Chunk workingChunk;
        private Chunk head;
        private Chunk tail;
        public WeakReference gcSignal;
        private object guard;

        private class Chunk
        {
            private object marker;

            public Chunk()
            {
                Marker = new object();
            }
            public object Marker {
                get
                {
                    return this.marker;
                }
                set
                {
                    this.marker = value;
                    if (value != null) this.Reference = new WeakReference(value, false);
                }
            }
            public WeakReference Reference { get; private set; }
            public ConcurrentStack<byte[]> free = new ConcurrentStack<byte[]>();
            public ConcurrentStack<byte[]> used = new ConcurrentStack<byte[]>();
            public Chunk prev;
            public Chunk next;
        }

        public WeakRefObjectPool(int blocksize, IObjectPoolMonitor2 monitor = null, TimeSpan? monitorWriteInterval = null)
        {
            this.blocksize = blocksize;
            this.monitor = monitor;
            if (this.monitor != null && monitorWriteInterval.HasValue)
            {
                this.periodicMonitoring = new PeriodicAction(monitorWriteInterval.Value, this.ReportObjectPoolStatistics);
            }
            this.workingChunk = new Chunk();
            this.gcSignal = new WeakReference(new object(), false);
            this.guard = new object();
            this.totalObjects = 0;
        }

        /// <summary>
        /// Allocates a pooled resource
        /// </summary>
        /// <returns></returns>
        public virtual DataBlock Allocate()
        {
            byte[] resource;

            (Chunk active, object marker) chunk = this.GetWorkingChunk();

            //if couldn't pop a resource from the pool
            if (!chunk.active.free.TryPop(out resource))
            {
                // check to see if gc has fired, which signals that resources may be free so we should cycle chunk.
                if(!gcSignal.IsAlive)
                {
                    SafeCycleChunk();
                }
                chunk = this.GetWorkingChunk();
                // if nothing was freed, or we failed to pop a resource again, allocate new one
                if (!chunk.active.free.TryPop(out resource))
                {
                    // create a new resource using factoryFunc from outside of the pool
                    resource = new byte[this.blocksize];
                    Interlocked.Increment(ref this.totalObjects);
                }
            }
            var block = new DataBlock(chunk.marker, resource);
            chunk.active.used.Push(resource);
            this.monitor?.TrackObjectAllocated();
            lock(this.guard)
            {
                this.periodicMonitoring?.TryAction(DateTime.UtcNow);
            }
            return block;
        }

        private (Chunk, object) GetWorkingChunk()
        {
            Chunk activeChunk = this.workingChunk;
            object marker = activeChunk.Marker;

            // race condition, we got old chunk
            // Note, in testing, this never happens, but in theory it could so we account for it.
            while (marker is null)
            {
                SafeCycleChunk();
                activeChunk = this.workingChunk;
                marker = activeChunk.Marker;
            }
            return (activeChunk, marker);
        }

        private void SafeCycleChunk()
        {
            lock (this.guard)
            {
                if (!gcSignal.IsAlive)
                {
                    this.gcCount++;
                    CycleChunk();
                    this.gcSignal = new WeakReference(new object());
                }
            }
        }

        private void CycleChunk()
        {
            Chunk freeChunk;
            // find a free chunck, or allocate new chunk
            if (TryFindFreeChhunk(out freeChunk))
            {
                Interlocked.Increment(ref this.reusedChunks);
                Unlink(freeChunk);
                Swap(ref freeChunk.free, ref freeChunk.used);
                freeChunk.Marker = new object();
            }
            else
            {
                freeChunk = new Chunk();
            }

            // link working chunk to tail, so most recently used chunk is last checked
            LinkAtTail(this.workingChunk);

            // swap free chunk with working chunk
            Swap(ref this.workingChunk, ref freeChunk);

            // clear marker in old working chunk, so only structures hold marker
            //   when they are all cleaned up, gc will free marker, signaling these resources are available.
            freeChunk.Marker = null;
        }

        private void Unlink(Chunk chunk)
        {
            // chunk is neither head nor tail, simple unlink
            if (chunk.next != null && chunk.prev != null)
            {
                chunk.prev.next = chunk.next;
                chunk.next.prev = chunk.prev;
            } else
            {
                // if chunk is head, set head to next and unlink from next
                if (chunk == this.head)
                {
                    this.head = chunk.next;
                    if (chunk.next != null)
                    {
                        chunk.next.prev = null;
                    }
                }
                // if chunk is tail, set tail to prev, and unlink prev
                if (chunk == this.tail)
                {
                    this.tail = chunk.prev;
                    if (chunk.prev != null)
                    {
                        chunk.prev.next = null;
                    }
                }
            }
            chunk.next = null;
            chunk.prev = null;
        }

        private void LinkAtTail(Chunk chunk)
        {
            chunk.prev = this.tail;
            if (this.tail != null)
            {
                this.tail.next = this.workingChunk;
            } else
            {
                // if tail is unset, head will also be unset, so this chunk becomes head also
                this.head = this.workingChunk;
            }
            this.tail = chunk;
        }

        private bool TryFindFreeChhunk(out Chunk freeChunk)
        {
            freeChunk = this.head;
            while (freeChunk != null)
            {
                if(!freeChunk.Reference.IsAlive)
                {
                    return true;
                }
                freeChunk = freeChunk.next;
            }
            return false;
        }

        private void ReportObjectPoolStatistics()
        {
            GetCounts(out long available, out long claimed, out long chunks);
            this.monitor.Report2(chunks, this.reusedChunks, this.gcCount);
            this.monitor.Report(this.totalObjects, available, claimed);
        }

        private void GetCounts(out long available, out long claimed, out long chunks)
        {
            available = this.workingChunk.free.Count;
            claimed = this.workingChunk.used.Count;
            chunks = 1;
            Chunk chunk = this.head;
            while (chunk != null)
            {
                chunks++;
                if (!chunk.Reference.IsAlive)
                {
                    available += chunk.used.Count;
                }
                else
                {
                    claimed += chunk.used.Count;
                }
                chunk = chunk.next;
            }
        }

        // simple ref swap
        private static void Swap<TItem>(ref TItem obj1, ref TItem obj2)
        {
            TItem stash = obj1;
            obj1 = obj2;
            obj2 = stash;
        }
    }
}
