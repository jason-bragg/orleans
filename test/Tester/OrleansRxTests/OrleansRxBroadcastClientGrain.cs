
using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Rx;
using Orleans.Rx.AsyncRx;

namespace Tester.OrleansRxTests
{
    public interface IOrleansRxBroadcastClientGrain : IGrainWithGuidKey, IRxObserverGrain
    {
        Task Listen(Guid serverId);
        Task<List<string>> Report();
    }

    class ReactiveBroadcastClientGrain : RxObserverGrain, IOrleansRxBroadcastClientGrain
    {
        private IAsyncConnectableObservable<string> observable;
        private List<string> report;

        public async Task Listen(Guid serverId)
        {
            report = new List<string>();
            IOrleansRxBroadcastServerGrain server = GrainFactory.GetGrain<IOrleansRxBroadcastServerGrain>(serverId);
            observable = Subscribe<string>(server, "Broadcast");
            await observable.ToSynchronous(UseRx);
            await observable.Connect();
        }

        /// <summary>
        /// Write whatever RX stuff you want in here
        /// </summary>
        /// <param name="obs"></param>
        /// <returns></returns>
        private IDisposable UseRx(IObservable<string> obs)
        {
            return obs.Where(msg => !string.IsNullOrEmpty(msg))
                      .Subscribe((msg) => report.Add(msg));
        }

        public Task<List<string>> Report()
        {
            var blarg = Observable.Create<int>(async o =>
            {
                await Task.Delay(100);
                o.OnNext(1);
            });
            return Task.FromResult(report);
        }
    }
}
