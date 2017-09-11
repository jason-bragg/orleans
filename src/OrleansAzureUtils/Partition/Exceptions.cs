using System;
using System.Runtime.Serialization;
using Orleans.Runtime;
using System.Globalization;

namespace Orleans.AzureUtils
{
    [Serializable]
    public class InvalidPartitionException : OrleansException
    {
        private const string MessageFormat = "Row has invalid PartitionKey.  RowKey: {0}, PartitionKey: {1}, TableName: {2}";

        private string RowKey { get; set; }
        private string PartitionKey { get; set; }
        private string TableName { get; set; }

        public InvalidPartitionException() : this("Row has invalid PartitionKey") { }
        public InvalidPartitionException(string message) : base(message) { }
        public InvalidPartitionException(string message, Exception inner) : base(message, inner) { }

        public InvalidPartitionException(string rowKey, string partitionKey, string tableName)
            : this(String.Format(CultureInfo.InvariantCulture, MessageFormat, rowKey, partitionKey, tableName))
        {
            RowKey = rowKey;
            PartitionKey = partitionKey;
            TableName = tableName;
        }

        public InvalidPartitionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            RowKey = info.GetString("RowKey");
            PartitionKey = info.GetString("PartitionKey");
            TableName = info.GetString("TableName");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("RowKey", RowKey);
            info.AddValue("PartitionKey", PartitionKey);
            info.AddValue("TableName", TableName);
            base.GetObjectData(info, context);
        }
    }
}
