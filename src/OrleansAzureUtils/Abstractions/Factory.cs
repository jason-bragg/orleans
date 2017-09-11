using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.AzureUtils.Abstractions
{
    public delegate Task<CloudTable> TableFactory(string account, string tableName, TableRequestOptions createOption = null, TableRequestOptions operationOptions = null);
    public delegate Task<ITablePartition> TablePartitionFactory(string tableName, string partitionName);
}
