
using System.Threading.Tasks;
using Orleans;
using Orleans.Rx;

namespace Tester.OrleansRxTests
{
    public interface IOrleansRxBroadcastServerGrain : IGrainWithGuidKey, IRxObservableGrain
    {
        Task Tell(string message);
    }

    public class OrleansRxBroadcastServerGrain : RxObservableGrain, IOrleansRxBroadcastServerGrain
    {
        private IAsyncObserver<string> observer;

        public override async Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            observer = Publish<string>("Broadcast");
        }

        public Task Tell(string message)
        {
            return observer.OnNextAsync(message);
        }
    }
}
