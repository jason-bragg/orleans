using System;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Orleans.Providers.Streams.Common;

namespace Benchmarks.ObjectPool
{
    public class Monitor : IObjectPoolMonitor2
    {
        private int allocationCount;
        private int releaseCount;

        public bool Done { get; private set; }

        public void Report(long totalObjects, long availableObjects, long claimedObjects)
        {
            Console.WriteLine($"AllocationCount: {allocationCount}, ReleaseCount: {releaseCount}, TotalObjects: {totalObjects}, AvailableObjects: {availableObjects}, ClaimedObjects: {claimedObjects}");
            Done = true;
        }

        public void Report2(long chunks, long reusedChunks, long gcCount)
        {
            Console.WriteLine($"ChunksAllocation: {chunks}, ReusedChunks: {reusedChunks}, GCCount: {gcCount}");
        }

        public void TrackObjectAllocated()
        {
            Interlocked.Increment(ref allocationCount);
        }

        public void TrackObjectReleased()
        {
            Interlocked.Increment(ref releaseCount);
        }
    }

    public class PooledResource : PooledResource<PooledResource>
    {
        public byte[] Data = new byte[1024];
    }

    [MemoryDiagnoser]
    public class ObjectPoolBenchmarks
    {
        [Benchmark]
        public void OjbectPool()
        {
            const int concurrent = 1000;
            var monitor = new Monitor();
            var pool = new ObjectPool<PooledResource>(() => new PooledResource(), monitor, TimeSpan.FromMilliseconds(5000));

            IDisposable[] inUse = new IDisposable[concurrent];
            int index = 0;
            while (!monitor.Done)
            {
                index = index % concurrent;
                if (inUse[index] != null)
                {
                    inUse[index].Dispose();
                    inUse[index] = null;
                }
                inUse[index] = pool.Allocate();
                index++;
            }
            for (index = 0; index < concurrent; index++)
            {
                if (inUse[index] != null)
                {
                    inUse[index].Dispose();
                    inUse[index] = null;
                }
            }
        }

        [Benchmark]
        public void WeakRefObjectPool()
        {
            const int concurrent = 1000;
            var monitor = new Monitor();
            var pool = new WeakRefObjectPool(1024, monitor, TimeSpan.FromMilliseconds(5000));

            DataBlock[] inUse = new DataBlock[concurrent];
            int index = 0;
            while (!monitor.Done)
            {
                index = index % concurrent;
                inUse[index] = pool.Allocate();
                index++;
            }
        }
    }
}
