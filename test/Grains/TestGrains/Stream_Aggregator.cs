using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{
    [ImplicitStreamSubscription(Stream_AggregationOptions.Int_Aggregator_StreamNamespace)]
    public class IntStream_Aggregator : Grain, IIntStream_Aggregator
    {
        private readonly Stream_AggregationOptions options;
        private readonly ILogger logger;
        private int numConsumedItems;
        private IAsyncObserver<IEnumerable<int>> publisher;
        private List<int> aggregated;

        public IntStream_Aggregator(IOptions<Stream_AggregationOptions> options, ILogger<IntStream_Aggregator> logger)
        {
            this.options = options.Value;
            this.logger = logger;
            this.aggregated = new List<int>();
        }
        public override async Task OnActivateAsync()
        {
            this.numConsumedItems = 0;
            IStreamProvider streamProvider = base.GetStreamProvider(this.options.StreamProviderName);
            IAsyncObservable<int> consumer = streamProvider.GetStream<int>(this.GetPrimaryKey(), Stream_AggregationOptions.Int_Aggregator_StreamNamespace);
            StreamSubscriptionHandle<int> handle = await consumer.SubscribeAsync(Consume);
            this.publisher = streamProvider.GetStream<IEnumerable<int>>(this.GetPrimaryKey(), Stream_AggregationOptions.Int_Aggregation_StreamNamespace);
            this.logger.LogInformation("OnActivateAsync {IdentityString}. StreamIdentity {StreamIdentity}, Provider {Provider}", IdentityString, handle.StreamIdentity, handle.ProviderName);
        }

        public Task<int> GetNumberConsumed()
        {
            return Task.FromResult(numConsumedItems);
        }

        private async Task Consume(int value, StreamSequenceToken token)
        {
            this.numConsumedItems++;
            this.aggregated.Add(value);
            if (this.aggregated.Count > 1)
            {
                await this.publisher.OnNextAsync(this.aggregated);
                this.aggregated.Clear();
            }
        }
    }

    [ImplicitStreamSubscription(Stream_AggregationOptions.String_Aggregator_StreamNamespace)]
    public class StringStream_Aggregator : Grain, IStringStream_Aggregator
    {
        private readonly Stream_AggregationOptions options;
        private readonly ILogger logger;
        private int numConsumedItems;
        private IAsyncObserver<IEnumerable<string>> publisher;
        private List<string> aggregated;

        public StringStream_Aggregator(IOptions<Stream_AggregationOptions> options, ILogger<IntStream_Aggregator> logger)
        {
            this.options = options.Value;
            this.logger = logger;
            this.aggregated = new List<string>();
        }
        public override async Task OnActivateAsync()
        {
            this.numConsumedItems = 0;
            IStreamProvider streamProvider = base.GetStreamProvider(this.options.StreamProviderName);
            IAsyncObservable<string> consumer = streamProvider.GetStream<string>(this.GetPrimaryKey(), Stream_AggregationOptions.String_Aggregator_StreamNamespace);
            StreamSubscriptionHandle<string> handle = await consumer.SubscribeAsync(Consume);
            this.publisher = streamProvider.GetStream<IEnumerable<string>>(this.GetPrimaryKey(), Stream_AggregationOptions.String_Aggregation_StreamNamespace);
            this.logger.LogInformation("OnActivateAsync {IdentityString}. StreamIdentity {StreamIdentity}, Provider {Provider}", IdentityString, handle.StreamIdentity, handle.ProviderName);
        }

        public Task<int> GetNumberConsumed()
        {
            return Task.FromResult(numConsumedItems);
        }

        private async Task Consume(string value, StreamSequenceToken token)
        {
            this.numConsumedItems++;
            this.aggregated.Add(value);
            if (this.aggregated.Count > 1)
            {
                await this.publisher.OnNextAsync(this.aggregated);
                this.aggregated.Clear();
            }
        }
    }

    [ImplicitStreamSubscription(Stream_AggregationOptions.Int_Aggregation_StreamNamespace)]
    [ImplicitStreamSubscription(Stream_AggregationOptions.String_Aggregation_StreamNamespace)]
    public class Stream_Aggregation : Grain, IStream_Aggregation
    {
        private readonly Stream_AggregationOptions options;
        private readonly ILogger logger;
        private int numIntConsumed;
        private int numStringConsumed;

        public Stream_Aggregation(IOptions<Stream_AggregationOptions> options, ILogger<Stream_Aggregation> logger)
        {
            this.options = options.Value;
            this.logger = logger;
        }

        public override async Task OnActivateAsync()
        {
            this.numIntConsumed = 0;
            this.numStringConsumed = 0;
            IStreamProvider streamProvider = base.GetStreamProvider(this.options.StreamProviderName);
            
            // consume ints
            IAsyncObservable<IEnumerable<int>> intConsumer = streamProvider.GetStream<IEnumerable<int>>(this.GetPrimaryKey(), Stream_AggregationOptions.Int_Aggregation_StreamNamespace);
            StreamSubscriptionHandle<IEnumerable<int>> intConsumerHandle = await intConsumer.SubscribeAsync(ConsumeInts);
            this.logger.LogInformation("OnActivateAsync {IdentityString} consuming ints. StreamIdentity {StreamIdentity}, Provider {Provider}", IdentityString, intConsumerHandle.StreamIdentity, intConsumerHandle.ProviderName);

            // consume strings
            IAsyncObservable<IEnumerable<string>> stringConsumer = streamProvider.GetStream<IEnumerable<string>>(this.GetPrimaryKey(), Stream_AggregationOptions.String_Aggregation_StreamNamespace);
            StreamSubscriptionHandle<IEnumerable<string>> stringConsumerHandle = await stringConsumer.SubscribeAsync(ConsumeStrings);
            this.logger.LogInformation("OnActivateAsync {IdentityString} consuming strings. StreamIdentity {StreamIdentity}, Provider {Provider}", IdentityString, stringConsumerHandle.StreamIdentity, stringConsumerHandle.ProviderName);
        }


        public Task<Tuple<int,int>> GetNumberConsumed()
        {
            return Task.FromResult(Tuple.Create(this.numIntConsumed, this.numStringConsumed));
        }

        private Task ConsumeInts(IEnumerable<int> value, StreamSequenceToken token)
        {
            this.numIntConsumed += value.Count();
            return Task.CompletedTask;
        }

        private Task ConsumeStrings(IEnumerable<string> value, StreamSequenceToken token)
        {
            this.numStringConsumed += value.Count();
            return Task.CompletedTask;
        }
    }
}
