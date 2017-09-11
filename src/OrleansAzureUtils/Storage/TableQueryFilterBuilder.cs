
using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.AzureUtils
{
    /// <summary>
    /// Helper functions for building table queries.
    /// </summary>
    internal class TableQueryFilterBuilder
    {
        /// <summary>
        /// Builds query string to match partitionkey
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <returns></returns>
        public static string MatchPartitionKeyFilter(string partitionKey)
        {
            return TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
        }

        /// <summary>
        /// Builds query string to match rowkey
        /// </summary>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public static string MatchRowKeyFilter(string rowKey)
        {
            return TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey);
        }

        /// <summary>
        /// Builds a query string that matches a specific partitionkey and rowkey.
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        /// <returns></returns>
        public static string MatchPartitionKeyAndRowKeyFilter(string partitionKey, string rowKey)
        {
            return TableQuery.CombineFilters(MatchPartitionKeyFilter(partitionKey), TableOperators.And,
                                      MatchRowKeyFilter(rowKey));
        }
        
        /// <summary>
        /// Builds query string for all rows greater than or equal to rowkey
        /// </summary>
        /// <param name="rowKeyStart"></param>
        /// <returns></returns>
        public static string MatchGreaterOrEqualRowKeyFilter(string rowKeyStart)
        {
            return TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, rowKeyStart);
        }

        /// <summary>
        /// Builds query string for all rows less than or equal to rowkey
        /// </summary>
        /// <param name="rowKeyEnd"></param>
        /// <returns></returns>
        public static string MatchLessThanOrEqualRowKeyFilter(string rowKeyEnd)
        {
            return TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, rowKeyEnd);
        }

        /// <summary>
        /// Builds query string that matches a given partitionkey and rowkey range
        /// </summary>
        /// <param name="partitionKey"></param>
        /// <param name="rowKeyStart"></param>
        /// <param name="rowKeyEnd"></param>
        /// <returns></returns>
        public static string MatchPartitionKeyAndRowKeyRangeFilter(string partitionKey, string rowKeyStart, string rowKeyEnd)
        {
            return TableQuery.CombineFilters(MatchPartitionKeyFilter(partitionKey), TableOperators.And,
                                             TableQuery.CombineFilters(MatchGreaterOrEqualRowKeyFilter(rowKeyStart), TableOperators.And,
                                                                       MatchLessThanOrEqualRowKeyFilter(rowKeyEnd)));
        }
    }
}
