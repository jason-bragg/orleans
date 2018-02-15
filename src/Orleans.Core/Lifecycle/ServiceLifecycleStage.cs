
namespace Orleans
{
    /// <summary>
    /// Lifecycle stages of an orlean service.  Cluster Client, or Silo
    /// </summary>
    public static class ServiceLifecycleStage
    {
        /// <summary>
        /// First stage in service's lifecycle
        /// </summary>
        public const int First = int.MinValue;

        /// <summary>
        /// Initialize runtime
        /// </summary>
        public const int RuntimeInitialize = 2000;

        /// <summary>
        /// Start runtime services
        /// </summary>
        public const int RuntimeServices = 4000;

        /// <summary>
        /// Initialize runtime storage
        /// </summary>
        public const int RuntimeStorageServices = 6000;

        /// <summary>
        /// Start runtime services
        /// </summary>
        public const int RuntimeGrainServices = 8000;

        /// <summary>
        /// Start application layer services
        /// </summary>
        public const int ApplicationServices = 10000;

        /// <summary>
        /// Service is active.
        /// </summary>
        public const int Active = 20000;

        /// <summary>
        /// First stage in service's lifecycle
        /// </summary>
        public const int Last = int.MaxValue;
    }
}
