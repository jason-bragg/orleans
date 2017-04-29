using Orleans.Runtime;

namespace Orleans.Providers.Streams.Common
{
    /// <summary>
    /// Temporarty cache reporting utility, until more complete cache monitoring replaces it.
    /// TODO: Replace with cache monitor - jbragg
    /// </summary>
    public static class CacheReporting
    {
        /// <summary>
        /// Logs cache purge activity
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="purgeObservable"></param>
        /// <param name="itemsPurged"></param>
        /// <typeparam name="TCachedMessage"></typeparam>
        public static void ReportPurge<TCachedMessage>(Logger logger, IPurgeObservable<TCachedMessage> purgeObservable, int itemsPurged)
            where TCachedMessage : struct 
        {
            if (!logger.IsVerbose)
                return;
            int itemCountAfterPurge = purgeObservable.ItemCount;
            var itemCountBeforePurge = itemCountAfterPurge + itemsPurged;
            if (itemCountAfterPurge == 0)
            {
                logger.Verbose("BlockPurged: cache empty");
            }
            logger.Verbose($"BlockPurged: PurgeCount: {itemCountBeforePurge - itemCountAfterPurge}, CacheSize: {itemCountAfterPurge}");
        }
    }
}
