using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Transactions.Abstractions
{
    /// <summary>
    /// This interface provides the abstraction for various durable transaction log storages.
    /// </summary>
    public interface ITransactionLogStorage
    {
        ///// <summary>
        ///// Gets the first CommitRecord in the log.
        ///// </summary>
        ///// <returns>
        ///// The CommitRecord with the lowest LSN in the log, or null if there is none.
        ///// </returns>
        Task<CommitRecord> GetFirstCommitRecord();

        /// <summary>
        /// Returns the CommitRecord with LSN following the LSN of record returned by the last
        /// GetFirstcommitRecord() or GetNextCommitRecord() call.
        /// </summary>
        /// <returns>
        /// The next CommitRecord, or null if there is none.
        /// </returns>
        Task<CommitRecord> GetNextCommitRecord();

        /// <summary>
        /// Append the given records to the log in order
        /// </summary>
        /// <param name="commitRecords">Commit Records</param>
        /// <remarks>
        /// If an exception is thrown it is possible that a prefix of the records are persisted
        /// to the log.
        /// </remarks>
        Task Append(IEnumerable<CommitRecord> commitRecords);

        /// <summary>
        /// Truncates the transaction log from the start until the given LSN provided in the <paramref name="lsn"/> parameter.
        /// </summary>
        /// <param name="lsn">Last LSN until the log should be truncated, this value is inclusive.</param>
        /// <returns></returns>
        Task TruncateLog(long lsn);
    }
}
