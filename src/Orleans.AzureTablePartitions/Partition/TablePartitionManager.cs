using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureTablePartitions.Abstractions;

namespace Orleans.AzureTablePartitions
{
    /// <summary>
    /// Default implementation of IPartitionManager.
    /// </summary>
    public class TablePartitionManager : ITablePartitionManager
    {
        private readonly CloudTable table;

        public TablePartitionManager(CloudTable table)
        {
            this.table = table ?? throw new ArgumentNullException(nameof(table));
        }

        public ITablePartition GetPartition(string partitionKey)
        {
            return new TablePartition(this.table, partitionKey);
        }

        public async Task DeletePartition(string partitionKey)
        {
            ITablePartition partition = GetPartition(partitionKey);

            //Maximum operations for a single batch
            const int MaxOperations = 100;
            
            string pkFilter = TableQueryFilterBuilder.MatchPartitionKeyFilter(partitionKey);
            TableQuery query = new TableQuery().Where(pkFilter).Take(MaxOperations);

            TableContinuationToken token = null;
            while(true)
            {
                try
                {
                    TableQuerySegment segment = await this.table.ExecuteQuerySegmentedAsync(query, token);
                    token = segment.ContinuationToken;
                    if (segment.Results.Count == 0) return;

                    var deleteBatch = new TableBatchOperation();
                    foreach (ITableEntity entity in segment.Results)
                    {
                        deleteBatch.Delete(entity);
                    }
                    await table.ExecuteBatchAsync(deleteBatch);
                } catch(Exception)
                {
                    token = null;
                }
            }
        }
    }
}
