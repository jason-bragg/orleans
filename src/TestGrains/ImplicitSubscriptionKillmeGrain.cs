/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Streams;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    [StatelessWorker(1)]
    public abstract class ImplicitSubscriptionKillmeGrain : Grain, IImplicitSubscriptionKillmeGrain
    {
        public const int KillSignal = -1;

        public abstract string StreamProvider { get; }
        public abstract string StreamNamespace { get; }

        private Logger logger;
        private IAsyncStream<int> stream;
        private int counter;
        private StreamSubscriptionHandle<int> handle;

        public override async Task OnActivateAsync()
        {
            logger = base.GetLogger("ImplicitSubscriptionKillmeGrain " + base.IdentityString);
            logger.Info("OnActivateAsync");

            var streamProvider = GetStreamProvider(StreamProvider);
            stream = streamProvider.GetStream<int>(this.GetPrimaryKey(), StreamNamespace);

            handle = await stream.SubscribeAsync(OnNext);
        }

        public override async Task OnDeactivateAsync()
        {
            logger.Info("OnDeactivateAsync");

            if (handle != null)
            {
                await handle.UnsubscribeAsync();
                handle = null;
            }
        }

        private async Task OnNext(int evt, StreamSequenceToken token)
        {
            if (evt == KillSignal)
            {
                logger.Info("Received kill event");
                var producerGrain = GrainFactory.GetGrain<IIntStreamProducerGrain>(this.GetPrimaryKey());
                await producerGrain.ReportCount(counter);
                base.DeactivateOnIdle();
            }
            else
            {
                logger.Info("Received an event {0}", evt);
                counter++;
            }
        }
    }

    [ImplicitStreamSubscription("green")]
    public class GreenImplicitSubscriptionKillmeGrain : ImplicitSubscriptionKillmeGrain
    {
        public override string StreamProvider
        {
            get { return "AzureQueueProvider"; }
        }

        public override string StreamNamespace
        {
            get { return "green"; }
        }
    }

    [ImplicitStreamSubscription("yellow")]
    public class YellowImplicitSubscriptionKillmeGrain : ImplicitSubscriptionKillmeGrain
    {
        public override string StreamProvider
        {
            get { return "AzureQueueProvider"; }
        }

        public override string StreamNamespace
        {
            get { return "yellow"; }
        }
    }

    public class IntStreamProducerGrain : Grain, IIntStreamProducerGrain
    {
        private int counter;

        public async Task Produce(string streamNamespace, int count)
        {
            var streamProvider = GetStreamProvider("AzureQueueProvider");
            IAsyncObserver<int> observer = streamProvider.GetStream<int>(this.GetPrimaryKey(), streamNamespace);
            // send count events;
            for (int i = 0; i < count; i++)
            {
                await observer.OnNextAsync(i);
            }
            // send kill message
            await observer.OnNextAsync(ImplicitSubscriptionKillmeGrain.KillSignal);
        }

        public Task ReportCount(int reportedCounter)
        {
            counter = reportedCounter;
            return TaskDone.Done;
        }

        public Task<int> GetCounter()
        {
            return Task.FromResult(counter);
        }
    }
}