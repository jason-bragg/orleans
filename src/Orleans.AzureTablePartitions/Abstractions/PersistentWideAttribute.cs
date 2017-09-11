using System;

namespace Orleans.AzureTablePartitions.Abstractions
{
    /// <summary>
    /// This attribute marks properties to be persisted.  It bypasses the 64KB Table Storage column size limitations
    ///  by storing the property into multiple columns.  These additional columns will be named after the property,
    ///  appended with the ExtensionSuffix defined in the <see cref="AttributableTableEntity"/>.
    /// </summary>
    /// <remarks>
    /// WARNING: This attributed does not support <see cref="TableOperation.Merge"/>
    ///  or <see cref="TableOperation.InsertOrMerge"/> operations. 
    /// </remarks>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class PersistentWideAttribute : PersistentAttribute
    {
        public PersistentWideAttribute(string propertyConverterName = null)
            : base(propertyConverterName, true)
        {
        }
    }
}
