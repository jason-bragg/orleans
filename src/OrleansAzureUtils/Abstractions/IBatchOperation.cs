using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;

namespace Orleans.AzureUtils.Abstractions
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
    /// A batch operation embodies a set of modifications to be made to a partition.
    /// - Batch must be committed to the partition for operations to be applied.
    /// - Storage operations in a batch will either all succeed or all fail.
    /// - No more than 100 operations are supported per batch.
    /// </summary>
    public interface IBatchOperation
    {
        /// <summary>
        /// Set of operations contained within the transaction
        /// </summary>
        IEnumerable<Tuple<StorageOperation, ITableEntity>> Operations { get; }
    }
}
