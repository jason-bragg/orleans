using System;
using System.Threading.Tasks;
using Orleans;

namespace UnitTests.GrainInterfaces
{
    public interface ISampleStreaming_ProducerGrain : IGrainWithGuidKey
    {
        Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse);

        Task StartPeriodicProducing();

        Task StopPeriodicProducing();

        Task<int> GetNumberProduced();

        Task ClearNumberProduced();
        Task Produce();
    }

    public interface ISimpleStreamConsumer<T> : IGrainWithGuidKey
        where T : class
    {
        Task Subscribe(string streamProviderName);
        Task<T> Get();
    }

    public interface ISimpleStreamProducer<T> : IGrainWithGuidKey
    where T : class
    {
        Task Send(string streamProviderName, T value);
    }

    public interface ISampleStreaming_ConsumerGrain : IGrainWithGuidKey
    {
        Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse);

        Task StopConsuming();

        Task<int> GetNumberConsumed();
    }

    public interface ISampleStreaming_InlineConsumerGrain : ISampleStreaming_ConsumerGrain
    {
    }

    public interface IGrainWithGenericMethodsValue : IGrainWithGuidKey
    {
        ValueTask<int> ValueTaskMethod(bool useCache);
    }
}
