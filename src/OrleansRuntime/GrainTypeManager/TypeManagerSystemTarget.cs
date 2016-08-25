using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Runtime.Providers;
using Orleans.Runtime.Scheduler;

namespace Orleans.Runtime
{
    internal class TypeManagerSystemTarget : SystemTarget, ITypeManager, ISiloStatusListener
    {
        private static readonly IBackoffProvider RetryBackoff = new ExponentialBackoff(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), TimeSpan.FromSeconds(1));
        private readonly GrainTypeManager myGrainTypeManager;
        private readonly ISiloStatusOracle statusOracle;
        private readonly OrleansTaskScheduler scheduler;
        
        internal TypeManagerSystemTarget(SiloAddress myAddr, GrainTypeManager grainTypeManager, ISiloStatusOracle oracle, OrleansTaskScheduler scheduler)
            : base(Constants.TypeManagerId, myAddr)
        {
            if (grainTypeManager == null)
            {
                throw new ArgumentNullException("grainTypeManager");
            }
            if (oracle == null)
            {
                throw new ArgumentNullException("oracle");
            }
            if (scheduler == null)
            {
                throw new ArgumentNullException("scheduler");
            }
            myGrainTypeManager = grainTypeManager;
            statusOracle = oracle;
            this.scheduler = scheduler;
        }

        /// <summary>
        /// Aquire grain interface map for all grain types supported by hosted silo.
        /// </summary>
        /// <returns></returns>
        public Task<GrainInterfaceMap> GetSiloTypeCodeMap()
        {
            return Task.FromResult(myGrainTypeManager.GetTypeCodeMap());
        }

        /// <summary>
        /// Aquires grain interface map for all grain types supported across the entire cluster
        /// </summary>
        /// <returns></returns>
        public Task<IGrainTypeResolver> GetClusterTypeCodeMap()
        {
            return Task.FromResult<IGrainTypeResolver>(myGrainTypeManager.ClusterGrainInterfaceMap);
        }

        public Task<Streams.ImplicitStreamSubscriberTable> GetImplicitStreamSubscriberTable(SiloAddress silo)
        {
            Streams.ImplicitStreamSubscriberTable table = SiloProviderRuntime.Instance.ImplicitStreamSubscriberTable;
            if (null == table)
            {
                throw new InvalidOperationException("the implicit stream subscriber table is not initialized");
            }
            return Task.FromResult(table);
        }

        public void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            Dictionary<SiloAddress, SiloStatus> siloStatuses = statusOracle.GetApproximateSiloStatuses(true);
            ICollection<SiloAddress> silosInMap = myGrainTypeManager.GetSilosInMap();

            // for any silos in map but not active, update
            foreach (SiloAddress silo in silosInMap.Where(s => !siloStatuses.ContainsKey(s)))
            {
                UpdateGrainTypesMapForSiloWithRetries(silo);
            }

            // for any active silos not in map, update
            foreach (SiloAddress silo in siloStatuses.Keys.Where(s => !silosInMap.Contains(s)))
            {
                UpdateGrainTypesMapForSiloWithRetries(silo);
            }
        }

        private void UpdateGrainTypesMapForSiloWithRetries(SiloAddress siloAddress)
        {
            AsyncExecutorWithRetries.ExecuteWithRetries(i => scheduler.QueueTask(() => UpdateGrainTypesMapForSilo(siloAddress), SchedulingContext),
                    AsyncExecutorWithRetries.INFINITE_RETRIES, (exception, i) => true, Constants.INFINITE_TIMESPAN, RetryBackoff).Ignore();
        }

        private async Task UpdateGrainTypesMapForSilo(SiloAddress siloAddress)
        {
            switch (statusOracle.GetApproximateSiloStatus(siloAddress))
            {
                case SiloStatus.Active:
                    await UpdateSiloGrainInterfaceMap(siloAddress);
                    break;
                default:
                    myGrainTypeManager.ClearSiloGrainInterfaceMap(siloAddress);
                    break;
            }
        }

        private async Task UpdateSiloGrainInterfaceMap(SiloAddress siloAddress)
        {
            GrainInterfaceMap grainInterfaceMap;
            // if me, get from grain type manager.
            if (siloAddress.Equals(Silo))
            {
                grainInterfaceMap = myGrainTypeManager.GetTypeCodeMap();
            }
            else
            {
                ITypeManager typeManager = GetTypeMangerReference(siloAddress);
                grainInterfaceMap = await typeManager.GetSiloTypeCodeMap();
            }

            // check again before applying change to map, since state could have changed while we were getting the map.
            switch (statusOracle.GetApproximateSiloStatus(siloAddress))
            {
                case SiloStatus.Active:
                    myGrainTypeManager.SetSiloGrainInterfaceMap(siloAddress, grainInterfaceMap);
                    break;
                default:
                    myGrainTypeManager.ClearSiloGrainInterfaceMap(siloAddress);
                    break;
            }
        }

        private ITypeManager GetTypeMangerReference(SiloAddress silo)
        {
            return InsideRuntimeClient.Current.InternalGrainFactory.GetSystemTarget<ITypeManager>(Constants.TypeManagerId, silo);
        }
    }
}


