using System;
using Orleans.Runtime;

namespace Orleans.Streams.Core
{
    internal class StreamSubscriptionManagerCollection : IKeyedServiceCollection<string, IStreamSubscriptionManager>
    {
        public IStreamSubscriptionManager GetService(IServiceProvider services, string key)
        {
            StreamPubSubType pubsubType;
            if(Enum.TryParse<StreamPubSubType>(key, out pubsubType))
            {
                switch(pubsubType)
                {
                    case StreamPubSubType.ImplicitOnly:
                        return null;
                    case StreamPubSubType.ExplicitGrainBasedAndImplicit:
                    case StreamPubSubType.ExplicitGrainBasedOnly:
                        return GetManager(services, StreamPubSubType.ExplicitGrainBasedOnly.ToString());
                    default:
                        throw new NotImplementedException($"Unrecognized StreamPubSubType type. StreamPubSubType: {pubsubType}");
                }
            }
            return GetManager(services, key);
        }

        private IStreamSubscriptionManager GetManager(IServiceProvider services, string key)
        {
            var registrar = services.GetServiceByName<IStreamSubscriptionRegistrar>(key);
            return (registrar != null) ? new StreamSubscriptionManager(registrar) : null;
        }
    }
}
