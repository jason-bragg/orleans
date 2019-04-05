using System.Threading.Tasks;

namespace Orleans.Streams
{
    public enum AdvancedStorageReadResultCode
    {
        Success,

        Throttled,

        GeneralFailure,
    }

    public enum AdvancedStorageWriteResultCode
    {
        /// <summary>
        /// The write was committed successfully.
        /// </summary>
        Success,

        /// <summary>
        /// The write completed in an ambiguous manner in which the write could have committed
        /// successfully or unsuccessfully.
        /// </summary>
        /// <remarks>
        /// This could be any number of cases including but not limited to:
        ///   - Timeout
        ///   - Task Cancelled
        /// </remarks>
        Ambiguous,

        /// <summary>
        /// The write was rejected and not committed due to a conflict.
        /// </summary>
        /// <remarks>
        /// This could be any number of cases including but not limited to:
        ///   - Entity already exists when trying to insert
        ///   - ETag mismatch error
        /// </remarks>
        Conflict,

        /// <summary>
        /// The write was rejected and not committed due to being throttled for traffic.
        /// </summary>
        /// <remarks>
        /// This could be any number of cases including but not limited to:
        ///   - Storage throttling (HTTP 429)
        ///   - Storage HTTP 5xx
        /// </remarks>
        Throttled,

        /// <summary>
        /// The write was not committed due to an unclassified general error.
        /// </summary>
        GeneralFailure,
    }

    public interface IAdvancedStorage<TState>
    {
        TState State { get; set; }

        string ETag { get; }

        Task<AdvancedStorageReadResultCode> ReadStateAsync();

        Task<AdvancedStorageWriteResultCode> WriteStateAsync();
    }
}