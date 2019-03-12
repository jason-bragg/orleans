using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public static class StreamSubscriptionRegistrarExtensions
    {
        public static Task<StreamSubscription<Guid>> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionRegistrar<Guid,IStreamIdentity> registrar,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            Guid primaryKey,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix) as GrainReference;
            return registrar.Register(streamId, grainRef);
        }

        public static Task<StreamSubscription<Guid>> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            long primaryKey,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix) as GrainReference;
            return registrar.Register(streamId, grainRef);
        }

        public static Task<StreamSubscription<Guid>> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            string primaryKey,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithStringKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix) as GrainReference;
            return registrar.Register(streamId, grainRef);
        }

        public static Task<StreamSubscription<Guid>> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            Guid primaryKey,
            string keyExtension,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidCompoundKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, keyExtension, grainClassNamePrefix) as GrainReference;
            return registrar.Register(streamId, grainRef);
        }

        public static Task<StreamSubscription<Guid>> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionRegistrar<Guid, IStreamIdentity> registrar,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            long primaryKey,
            string keyExtension,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerCompoundKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, keyExtension, grainClassNamePrefix) as GrainReference;
            return registrar.Register(streamId, grainRef);
        }

        public static async Task<StreamSubscription<TSubscriptionId>> Register<TSubscriptionId, TStreamId>(
            this IStreamSubscriptionRegistrar<TSubscriptionId, TStreamId> registrar,
            TStreamId streamId,
            GrainReference consumer)
        {
            TSubscriptionId subscriptionId = registrar.CreateSubscription(streamId, consumer);
            await registrar.Register(streamId, subscriptionId, consumer);
            return new StreamSubscription<TSubscriptionId>(subscriptionId, consumer);
        }
    }

}
