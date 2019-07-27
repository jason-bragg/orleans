using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using Orleans;
using Orleans.Streams;
using BenchmarkGrainInterfaces.Streams;

namespace BenchmarkGrains.Streams
{
    public class WriteLoadGrain : Grain, IWriteLoadGrain
    {
        const int OneK = 1024;
        const int PayLoadInK = 16;
        public const string StreamProviderName = "WriteLoad";
        private const string StreamNamespace = "WriteLoad";
        private static readonly byte[] bytePayload = Enumerable.Range(0, OneK).Select(i => (byte)i).ToArray();
        private static readonly Payload payload = new Payload();
        private Task<Report> runTask;
        private bool end = false;

        [Serializable]
        public class Payload
        {
            public byte[][] payload;
            public Payload()
            {
                this.payload = Enumerable.Range(0, PayLoadInK).Select(_ => bytePayload).ToArray();
            }
        }


        public Task Generate(int run, int conncurrent)
        {
            this.runTask = RunGeneration(run, conncurrent);
            return Task.CompletedTask;
        }

        public async Task<Report> TryGetReport()
        {
            this.end = true;
            return await this.runTask;
        }

        private async Task<Report> RunGeneration(int run, int conncurrent)
        {
            IStreamProvider provider = GetStreamProvider(StreamProviderName);
            List<Pending> pendingWork = Enumerable.Range(run * conncurrent, conncurrent).Select(i => new Pending() { Sender = provider.GetStream<Payload>(GenerateStreamGuid(i),StreamNamespace)}).ToList();
            Report report = new Report();
            Stopwatch sw = Stopwatch.StartNew();
            while (!this.end)
            {
                foreach(Pending pending in pendingWork.Where(t => t.PendingCall == null))
                {
                    pending.PendingCall = pending.Sender.OnNextAsync(payload);
                }
                await ResolvePending(pendingWork, report);
            }
            await ResolvePending(pendingWork, report, true);
            sw.Stop();
            report.Elapsed = sw.Elapsed;
            return report;
        }

        private async Task ResolvePending(List<Pending> pendingWork, Report report, bool all = false)
        {
            try
            {
                if(all)
                {
                    await Task.WhenAll(pendingWork.Select(p => p.PendingCall).Where(t => t!=null));
                }
                else
                {
                    await Task.WhenAny(pendingWork.Select(p => p.PendingCall).Where(t => t != null));
                }
            } catch (Exception) {}
            foreach (Pending pending in pendingWork.Where(p => p.PendingCall != null))
            {
                if (pending.PendingCall.IsFaulted || pending.PendingCall.IsCanceled)
                {
                    report.Failed++;
                    pending.PendingCall = null;
                }
                else if (pending.PendingCall.IsCompleted)
                {
                    report.Succeeded++;
                    pending.PendingCall = null;
                }
            }
        }

        private Guid GenerateStreamGuid(int index)
        {
            Int64 index64 = index;
            index64 = (index64 << 32) | index64; 
            byte[] bits = BitConverter.GetBytes(index64);
            return new Guid(index, 0, 0, bits);
        }
        
        private class Pending
        {
            public IAsyncObserver<Payload> Sender { get; set; }
            public Task PendingCall { get; set; }
        }
    }
}
