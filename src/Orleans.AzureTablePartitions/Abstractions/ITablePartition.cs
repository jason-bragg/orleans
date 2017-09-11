using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.AzureTablePartitions.Abstractions
{
    /// <summary>
    /// Interface for managing rows in a partition.
    /// </summary>
    public interface ITablePartition
    {
        /// <summary>
        /// Retrieve a single row by rowkey
        /// </summary>
        /// <typeparam name="TElement">The element to convert to</typeparam>
        /// <param name="rowKey">The row key of the element to look up</param>
        /// <returns>The element</returns>
        Task<TElement> QueryRow<TElement>(string rowKey)
            where TElement : class, ITableEntity, new();

        /// <summary>
        /// Retrieve a range of rows by rowkey range
        /// </summary>
        /// <typeparam name="TElement">The element to convert to</typeparam>
        /// <param name="rowKeyStart">The start of the row key range</param>
        /// <param name="rowKeyEnd">The end of the row key range</param>
        /// <returns>The rows of type TElement</returns>
        Task<IEnumerable<TElement>> QueryRowRange<TElement>(string rowKeyStart, string rowKeyEnd)
            where TElement : class, ITableEntity, new();

        /// <summary>
        /// Retrieve a range of rows by rowkey range
        /// </summary>
        /// <typeparam name="TElement">The element to convert to</typeparam>
        /// <param name="rowKeyStart">The start of the row key range</param>
        /// <param name="rowKeyEnd">The end of the row key range</param>
        /// <param name="takeLimit">Number of items that should be returned by storage</param>
        /// <returns>The rows of type TElement</returns>
        Task<IEnumerable<TElement>> QueryRowRange<TElement>(string rowKeyStart, string rowKeyEnd, int takeLimit)
            where TElement : class, ITableEntity, new();

        /// <summary>
        /// Commits a set of changes recorded in the provided transaction.
        /// </summary>
        /// <param name="transaction">The transaction to commit</param>
        Task Commit(IStorageTransaction transaction);
    }
}
