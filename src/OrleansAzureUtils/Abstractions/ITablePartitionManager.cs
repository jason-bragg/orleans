using System.Threading.Tasks;

namespace Orleans.AzureUtils.Abstractions
{
    /// <summary>
    /// Interface for aquiring a partition.
    /// </summary>
    public interface ITablePartitionManager
    {
        /// <summary>
        /// Aquire a partion
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        ITablePartition GetPartition(string partitionKey);

        /// <summary>
        /// Delete partition.  Removes all entities in the partition.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        Task DeletePartition(string partitionKey);
    }
}
