using Orleans.Streams.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Streams.PubSub
{
    public static class StreamSubscriptionManagerExtensions
    {
        public static Task<StreamSubscription> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionManager manager,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            string streamProviderName,
            Guid primaryKey,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix) as GrainReference;
            return manager.AddSubscription(streamProviderName, streamId, grainRef);
        }

        public static Task<StreamSubscription> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionManager manager,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            string streamProviderName,
            long primaryKey,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix) as GrainReference;
            return manager.AddSubscription(streamProviderName, streamId, grainRef);
        }

        public static Task<StreamSubscription> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionManager manager,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            string streamProviderName,
            string primaryKey,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithStringKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, grainClassNamePrefix) as GrainReference;
            return manager.AddSubscription(streamProviderName, streamId, grainRef);
        }

        public static Task<StreamSubscription> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionManager manager,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            string streamProviderName,
            Guid primaryKey,
            string keyExtension,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithGuidCompoundKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, keyExtension, grainClassNamePrefix) as GrainReference;
            return manager.AddSubscription(streamProviderName, streamId, grainRef);
        }

        public static Task<StreamSubscription> AddSubscription<TGrainInterface>(
            this IStreamSubscriptionManager manager,
            IGrainFactory grainFactory,
            IStreamIdentity streamId,
            string streamProviderName,
            long primaryKey,
            string keyExtension,
            string grainClassNamePrefix = null)
            where TGrainInterface : IGrainWithIntegerCompoundKey
        {
            var grainRef = grainFactory.GetGrain<TGrainInterface>(primaryKey, keyExtension, grainClassNamePrefix) as GrainReference;
            return manager.AddSubscription(streamProviderName, streamId, grainRef);
        }

        public static bool TryGetStreamSubscrptionManager(this IStreamProvider streamProvider, out IStreamSubscriptionManager manager)
        {
            manager = (streamProvider as IManageableSreamProvider)?.GetStreamSubscriptionManager();
            return manager != null;
        }
    }
 }
