using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureTablePartitions.Abstractions;

namespace Orleans.AzureTablePartitions
{
    /// <summary>
    /// This is the default implementation of IPartition.
    /// </summary>
    internal class TablePartition : ITablePartition
    {
        public string StorageIdentity { get { return this.table.Uri != null ? this.table.Uri.OriginalString : "<null>"; } }
        private readonly CloudTable table;
        private readonly string partitionKey;

        public TablePartition(CloudTable table, string partitionKey)
        {
            this.table = table;
            this.partitionKey = partitionKey;
        }

        /// <summary>
        /// Retrieve a single row by rowkey
        /// </summary>
        /// <typeparam name="TElement">The element to convert to</typeparam>
        /// <param name="rowKey">The row key of the element to look up</param>
        /// <returns>The element</returns>
        public async Task<TElement> QueryRow<TElement>(string rowKey)
            where TElement : class, ITableEntity, new()
        {
            string queryString = TableQueryFilterBuilder.MatchPartitionKeyAndRowKeyFilter(this.partitionKey, rowKey);
            var query = new TableQuery<TElement>().Where(queryString);
            TableQuerySegment<TElement> segment = await this.table.ExecuteQuerySegmentedAsync(query,null);
            return segment.FirstOrDefault();
        }

        /// <summary>
        /// Retrieve a range of rows by rowkey range
        /// </summary>
        /// <typeparam name="TElement">The element to convert to</typeparam>
        /// <param name="rowKeyStart">The start of the row key range</param>
        /// <param name="rowKeyEnd">The end of the row key range</param>
        /// <returns>The rows of type TElement</returns>
        public Task<IEnumerable<TElement>> QueryRowRange<TElement>(string rowKeyStart, string rowKeyEnd)
            where TElement : class, ITableEntity, new()
        {
            if (string.IsNullOrEmpty(rowKeyStart)) throw new ArgumentNullException(nameof(rowKeyStart));
            if (string.IsNullOrEmpty(rowKeyEnd)) throw new ArgumentNullException(nameof(rowKeyEnd));
            return this.QueryRowRangeInternal<TElement>(rowKeyStart, rowKeyEnd, takeLimit: null);
        }

        /// <summary>
        /// Retrieve a range of rows by rowkey range
        /// </summary>
        /// <typeparam name="TElement">The element to convert to</typeparam>
        /// <param name="rowKeyStart">The start of the row key range</param>
        /// <param name="rowKeyEnd">The end of the row key range</param>
        /// <param name="takeLimit">Number of items that should be returned by storage</param>
        /// <returns>The rows of type TElement</returns>
        public Task<IEnumerable<TElement>> QueryRowRange<TElement>(string rowKeyStart, string rowKeyEnd, int takeLimit)
            where TElement : class, ITableEntity, new()
        {
            if (string.IsNullOrEmpty(rowKeyStart)) throw new ArgumentNullException(nameof(rowKeyStart));
            if (string.IsNullOrEmpty(rowKeyEnd)) throw new ArgumentNullException(nameof(rowKeyEnd));
            return this.QueryRowRangeInternal<TElement>(rowKeyStart, rowKeyEnd, takeLimit);
        }

        private async Task<IEnumerable<TElement>> QueryRowRangeInternal<TElement>(string rowKeyStart, string rowKeyEnd, int? takeLimit)
            where TElement : class, ITableEntity, new()
        {
            string queryString = TableQueryFilterBuilder.MatchPartitionKeyAndRowKeyRangeFilter(this.partitionKey, rowKeyStart, rowKeyEnd);
            TableQuery<TElement> query = new TableQuery<TElement>().Where(queryString).Take(takeLimit);
            var results = new List<TElement>();
            TableContinuationToken token = null;
            do
            {
                TableQuerySegment<TElement> segment = await this.table.ExecuteQuerySegmentedAsync(query, token);
                token = segment.ContinuationToken;

                if (takeLimit.HasValue)
                {
                    results.AddRange(segment.Results.Take(takeLimit.Value - results.Count));
                }
                else
                {
                    results.AddRange(segment.Results);
                }
            } while (token != null && (!takeLimit.HasValue || results.Count < takeLimit.Value));

            return results;
        }

        /// <summary>
        /// Commits a set of changes recorded in the provided transaction.
        /// </summary>
        /// <param name="transaction">The transaction to commit</param>
        public async Task Commit(IStorageTransaction transaction)
        {
            if (transaction == null) throw new ArgumentNullException(nameof(transaction), @"Cannot commit an empty transaction to storage");

            OperationContext operationContext = new OperationContext();
            TableBatchOperation batchOperation = BuildBatchOperation(transaction);

            await this.table.ExecuteBatchAsync(batchOperation);
        }

        private TableBatchOperation BuildBatchOperation(IStorageTransaction transaction)
        {
            var batchOperation = new TableBatchOperation();

            foreach (var op in transaction.Operations)
            {
                ITableEntity entity = op.Item2;
                switch (op.Item1)
                {
                    case StorageOperation.Insert:
                        {
                            this.SetPartitionKeyIfNotSet(ref entity);
                            this.ValidatePartitionKey(entity, transaction.ContextId);
                            batchOperation.Insert(entity);
                            break;
                        }
                    case StorageOperation.Update:
                        {
                            this.ValidatePartitionKey(entity, transaction.ContextId);
                            batchOperation.Replace(entity);
                            break;
                        }
                    case StorageOperation.InsertOrReplace:
                        {
                            this.SetPartitionKeyIfNotSet(ref entity);
                            this.ValidatePartitionKey(entity, transaction.ContextId);
                            batchOperation.InsertOrReplace(entity);
                            break;
                        }
                    case StorageOperation.Delete:
                        {
                            this.ValidatePartitionKey(entity, transaction.ContextId);
                            batchOperation.Delete(entity);
                            break;
                        }
                    default:
                        throw new ArgumentOutOfRangeException("Invalid Operation Value", op.Item1, $"An invalid operation was entered within a transaction in this partition with key: {this.partitionKey}");
                }
            }

            return batchOperation;
        }

        private void SetPartitionKeyIfNotSet(ref ITableEntity entity)
        {
            if (string.IsNullOrEmpty(entity.PartitionKey))
            {
                entity.PartitionKey = this.partitionKey;
            }
        }

        private void ValidatePartitionKey(ITableEntity entity, Guid contextId)
        {
            if (!this.IsValidPartitionKey(entity))
            {
                throw new InvalidPartitionException(entity.RowKey, entity.PartitionKey, this.table.Name, contextId);
            }
        }

        private bool IsValidPartitionKey(ITableEntity entity)
        {
            return this.partitionKey.Equals(entity.PartitionKey);
        }
    }
}
