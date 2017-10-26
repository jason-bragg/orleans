using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace UnitTests.GrainInterfaces
{
    public interface IMultipleSubscriptionConsumerGrain : IGrainWithGuidKey
    {
        Task<GuidId> BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse);

        Task Resume(GuidId subscription, Guid streamId, string streamNamespace, string providerToUse);

        Task StopConsuming(GuidId subscription);

        Task<IList<GuidId>> GetAllSubscriptions(Guid streamId, string streamNamespace, string providerToUse);

        Task<Dictionary<GuidId, Tuple<int,int>>> GetNumberConsumed();

        Task ClearNumberConsumed();

        Task Deactivate();
    }
}
