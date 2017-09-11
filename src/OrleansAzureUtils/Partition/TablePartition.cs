using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureUtils.Abstractions;

namespace Orleans.AzureUtils
{
    /// <summary>
    /// This is the default implementation of IPartition.
    /// </summary>
    internal class TablePartition : ITablePartition
    {
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
        /// Commits a set of changes recorded in the provided batch.
        /// </summary>
        /// <param name="operation">The batch of operations to commit</param>
        public async Task Commit(IBatchOperation batch)
        {
            if (batch == null) throw new ArgumentNullException(nameof(batch), @"Cannot commit an empty batch to storage");

            OperationContext operationContext = new OperationContext();
            TableBatchOperation batchOperation = BuildBatchOperation(batch);

            await this.table.ExecuteBatchAsync(batchOperation);
        }

        private TableBatchOperation BuildBatchOperation(IBatchOperation batch)
        {
            var batchOperation = new TableBatchOperation();

            foreach (var op in batch.Operations)
            {
                ITableEntity entity = op.Item2;
                switch (op.Item1)
                {
                    case StorageOperation.Insert:
                        {
                            this.SetPartitionKeyIfNotSet(ref entity);
                            this.ValidatePartitionKey(entity);
                            batchOperation.Insert(entity);
                            break;
                        }
                    case StorageOperation.Update:
                        {
                            this.ValidatePartitionKey(entity);
                            batchOperation.Replace(entity);
                            break;
                        }
                    case StorageOperation.InsertOrReplace:
                        {
                            this.SetPartitionKeyIfNotSet(ref entity);
                            this.ValidatePartitionKey(entity);
                            batchOperation.InsertOrReplace(entity);
                            break;
                        }
                    case StorageOperation.Delete:
                        {
                            this.ValidatePartitionKey(entity);
                            batchOperation.Delete(entity);
                            break;
                        }
                    default:
                        throw new ArgumentOutOfRangeException("Invalid Operation Value", op.Item1, $"An invalid operation was entered within a batch in this partition with key: {this.partitionKey}");
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

        private void ValidatePartitionKey(ITableEntity entity)
        {
            if (!this.IsValidPartitionKey(entity))
            {
                throw new InvalidPartitionException(entity.RowKey, entity.PartitionKey, this.table.Name);
            }
        }

        private bool IsValidPartitionKey(ITableEntity entity)
        {
            return this.partitionKey.Equals(entity.PartitionKey);
        }
    }
}
