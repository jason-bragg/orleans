using System;

namespace Orleans.AzureTablePartitions.Abstractions
{
    /// <summary>
    /// Attribute that marks properties to be persisted
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class PersistentAttribute : Attribute
    {
        private const bool defaultExpandableValue = false;

        /// <summary>
        /// Name of property converter to use for this property
        /// </summary>
        public string PropertyConverterName { get; }

        /// <summary>
        /// Set to true if the field can be stored in more than a single azure table property if data
        ///   size larger than can be stored in single property.
        /// </summary>
        public bool Expandable { get; }

        public PersistentAttribute(string propertyConverterName = null)
            : this(propertyConverterName, defaultExpandableValue)
        {
        }

        protected PersistentAttribute(string propertyConverterName, bool expandable)
        {
            this.PropertyConverterName = propertyConverterName;
            this.Expandable = expandable;
        }
    }
}
