using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace Orleans.AzureTablePartitions.Abstractions
{
    /// <summary>
    /// The list of available TableOperations supported
    /// </summary>
    /// <remarks>
    /// Note: if adding 'Merge' support, please see impact described in <see cref="AttributableTableEntity"/>
    /// </remarks>
    public enum StorageOperation
    {
        Insert,
        Update,
        Delete,
        InsertOrReplace,
    }

    /// <summary>
    /// A storage transaction embodies a set of modifications to be made to a partition
    /// - Transaction must be committed to the partition for operations to be applied.
    /// - Operations in a transaction will either all succeed or all fail.
    /// - No more than 100 operations are supported per transaction.
    /// </summary>
    public interface IStorageTransaction
    {
        /// <summary>
        /// Set of operations contained within the transaction
        /// </summary>
        IEnumerable<Tuple<StorageOperation, ITableEntity>> Operations { get; }

        /// <summary>
        /// A unique identifier for this transaction
        /// </summary>
        Guid ContextId { get; }
    }
}
