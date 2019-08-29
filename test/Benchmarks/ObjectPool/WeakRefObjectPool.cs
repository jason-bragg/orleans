
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

            Chunk activeChunk = this.workingChunk;

            //if couldn't pop a resource from the pool
            if (!activeChunk.free.TryPop(out resource))
            {
                // check to see if gc has fired, which signals that resources may be free so we should cycle chunk.
                if(!gcSignal.IsAlive)
                {
                    lock(this.guard)
                    {
                        if (!gcSignal.IsAlive)
                        {
                            Interlocked.Increment(ref this.gcCount);
                            CycleChunk();
                            this.gcSignal = new WeakReference(new object());
                        }
                    }
                }
                activeChunk = this.workingChunk;
                // if nothing was freed, or we failed to pop a resource again, allocate new one
                if (!activeChunk.free.TryPop(out resource))
                {
                    // create a new resource using factoryFunc from outside of the pool
                    resource = new byte[this.blocksize];
                    Interlocked.Increment(ref this.totalObjects);
                }
            }
            var block = new DataBlock(activeChunk.Marker, resource);
            activeChunk.used.Push(resource);
            this.monitor?.TrackObjectAllocated();
            lock(this.guard)
            {
                this.periodicMonitoring?.TryAction(DateTime.UtcNow);
            }
            return block;
        }

        private void CycleChunk()
        {
            Chunk freeChunk;
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
            this.workingChunk.prev = this.tail;
            if(this.tail != null)
            {
                this.tail.next = this.workingChunk;
            }
            this.tail = this.workingChunk;
            if(this.head == null)
            {
                head = this.workingChunk;
            }
            this.workingChunk.Marker = null;
            this.workingChunk = freeChunk;
        }

        private void Unlink(Chunk chunk)
        {
            if (chunk.next != null && chunk.prev != null)
            {
                chunk.prev.next = chunk.next;
                chunk.next.prev = chunk.prev;
                chunk.next = null;
                chunk.prev = null;
                return;
            }
            if (chunk == this.head)
            {
                this.head = chunk.next;
                if(chunk.next != null)
                {
                    chunk.next.prev = null;
                    chunk.next = null;
                }
            }
            if (chunk == this.tail)
            {
                this.tail = chunk.prev;
                if (chunk.prev != null)
                {
                    chunk.prev.next = null;
                    chunk.prev = null;
                }
            }
        }

        private static void Swap<TItem>(ref TItem obj1, ref TItem obj2)
        {
            TItem stash = obj1;
            obj1 = obj2;
            obj2 = stash;
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
    }
}
