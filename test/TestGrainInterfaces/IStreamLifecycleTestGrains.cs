using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Streams;
using Orleans.Runtime;

namespace UnitTests.GrainInterfaces
{
    public interface IStreamLifecycleConsumerGrain : IGrainWithGuidKey
    {
        Task<int> GetReceivedCount();
        Task<int> GetErrorsCount();

        Task Ping();
        Task BecomeConsumer(Guid streamId, string streamNamespace, string providerName);
        Task TestBecomeConsumerSlim(Guid streamId, string streamNamespace, string providerName);
        Task RemoveConsumer(Guid streamId, string streamNamespace, string providerName, GuidId subscription);
        Task ClearGrain();
    }

    public interface IFilteredStreamConsumerGrain : IStreamLifecycleConsumerGrain
    {
        Task BecomeConsumer(Guid streamId, string streamNamespace, string providerName, bool sendEvensOnly);
        Task SubscribeWithBadFunc(Guid streamId, string streamNamespace, string providerName);
    }

    public interface IStreamLifecycleProducerGrain : IGrainWithGuidKey
    {
        Task<int> GetSendCount();
        Task<int> GetErrorsCount();

        Task Ping();

        Task BecomeProducer(Guid streamId, string streamNamespace, string providerName);
        Task ClearGrain();

        Task DoDeactivateNoClose();

        Task SendItem(int item);
    }
}
