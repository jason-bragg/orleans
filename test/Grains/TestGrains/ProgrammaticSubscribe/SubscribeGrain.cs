using Orleans.Streams;
using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

namespace UnitTests.Grains.ProgrammaticSubscribe
{
    public interface ISubscribeGrain : IGrainWithGuidKey
    {
        Task<bool> CanGetSubscriptions(string providerName);
    }

    public class SubscribeGrain : Grain, ISubscribeGrain
    {
        public Task<bool> CanGetSubscriptions(string providerName)
        {
            IStreamSubscriptionManifest<Guid,IStreamIdentity> manaifest = this.ServiceProvider.GetServiceByName<IStreamSubscriptionManifest<Guid, IStreamIdentity>>(providerName);
            return Task.FromResult(manaifest != null);
        }
    }

    [Serializable]
    public class FullStreamIdentity : IStreamIdentity
    {
        public FullStreamIdentity(Guid streamGuid, string streamNamespace, string providerName)
        {
            Guid = streamGuid;
            Namespace = streamNamespace;
            this.ProviderName = providerName;
        }

        public string ProviderName;
        /// <summary>
        /// Stream primary key guid.
        /// </summary>
        public Guid Guid { get; }

        /// <summary>
        /// Stream namespace.
        /// </summary>
        public string Namespace { get; }
    }
}
