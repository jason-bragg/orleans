using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureTablePartitions.Abstractions;

namespace Orleans.AzureTablePartitions
{
    /// <summary>
    /// Table entity that only stores fields marked to be persisted.  It can persist any type natively
    ///  supported by table storage or any custom types added to the Property converter.
    /// </summary>
    public class AttributableTableEntity : ITableEntity
    {
        private const string DefaultExtensionSuffix = "_Expanded_";
        private readonly IPropertyConverter propertyConverter;

        public AttributableTableEntity()
            : this(DefaultPropertyConverter.Instance)
        {
        }

        protected AttributableTableEntity(IPropertyConverter propertyConverter)
        {
            this.propertyConverter = propertyConverter;
        }

        /// <summary>
        /// Gets the suffix to use for overflow columns on large properties
        /// </summary>
        protected virtual string ExtensionSuffix => DefaultExtensionSuffix;

        public string PartitionKey { get; set; }
        public virtual string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public string ETag { get; set; }

        private class PersistentProperty
        {
            public PropertyInfo Property;
            public PersistentAttribute Attribute;
        }
        private static readonly ConcurrentDictionary<Type, PersistentProperty[]> FilteredPropertiesMap = new ConcurrentDictionary<Type, PersistentProperty[]>();

        public virtual void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            // Group and combine properties
            var combinedProperties = properties.GroupBy(this.BasePropertyName)
                                               .Select(PropertyCombiner)
                                               .ToDictionary(SelectKey, SelectValue);

            foreach (PersistentProperty persistentPropery in this.GetFilteredProperties())
            {
                if (combinedProperties.TryGetValue(persistentPropery.Property.Name, out EntityProperty entityProperty))
                {
                    this.propertyConverter.ConvertFromStorage(this, persistentPropery.Property, entityProperty);
                }
            }
        }

        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var storageProperties = new Dictionary<string, EntityProperty>();

            foreach (PersistentProperty persistentProperty in this.GetFilteredProperties())
            {
                if (persistentProperty.Property.Name.Contains(this.ExtensionSuffix))
                {
                    throw new ArgumentException("Property names should not contain extension suffix.", persistentProperty.Property.Name);
                }

                this.propertyConverter.ConvertToStorage(this, persistentProperty.Property, out EntityProperty storageProperty);

                switch (storageProperty.PropertyType)
                {
                    case EdmType.String:
                        WriteStringProperty(persistentProperty, storageProperty, storageProperties);
                        break;
                    case EdmType.Binary:
                        WriteBinaryProperty(persistentProperty, storageProperty, storageProperties);
                        break;
                    default:
                        storageProperties.Add(persistentProperty.Property.Name, storageProperty);
                        break;
                }
            }

            if (storageProperties.Count > PropertyConstants.MaxProperties)
            {
                throw new IndexOutOfRangeException("Property count exceeds maximum Azure Table Storage Row Limit of 252");
            }

            return storageProperties;
        }

        private void WriteStringProperty(PersistentProperty persistentProperty, EntityProperty storageProperty, Dictionary<string, EntityProperty> storageProperties)
        {
            if (!string.IsNullOrEmpty(storageProperty.StringValue))
            {
                string value = storageProperty.StringValue;
                // if bit enough to expand and expandable, expand
                if (value.Length > PropertyConstants.MaxStringLength && persistentProperty.Attribute.Expandable)
                {
                    // The Property needs to be split apart as it won't fit into one cell
                    this.Expand(persistentProperty, value, storageProperty, storageProperties);
                }
            }
            storageProperties.Add(persistentProperty.Property.Name, storageProperty);
        }

        private void WriteBinaryProperty(PersistentProperty persistentProperty, EntityProperty storageProperty, Dictionary<string, EntityProperty> storageProperties)
        {
            if (storageProperty.BinaryValue != null)
            {
                byte[] value = storageProperty.BinaryValue;
                // if bit enough to expand and expandable, expand
                if (value.Length > PropertyConstants.MaxPropertySize && persistentProperty.Attribute.Expandable)
                {
                    // The Property needs to be split apart as it won't fit into one cell
                    this.Expand(persistentProperty, value, storageProperty, storageProperties);
                }
            }
            storageProperties.Add(persistentProperty.Property.Name, storageProperty);
        }

        private PersistentProperty[] GetFilteredProperties()
        {
            return FilteredPropertiesMap.GetOrAdd(this.GetType(), t => this.BuildFilteredProperties().ToArray());
        }

        private IEnumerable<PersistentProperty> BuildFilteredProperties()
        {
            IEnumerable<PropertyInfo> properties = this.GetType()
                                                       .GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                                                       .Where(propertyInfo => (propertyInfo.Name != "PartitionKey") &&
                                                                              (propertyInfo.Name != "RowKey") &&
                                                                              (propertyInfo.Name != "Timestamp") &&
                                                                              (propertyInfo.Name != "ETag"));
            foreach (PropertyInfo propertyInfo in properties)
            {
                var attribute = propertyInfo.GetCustomAttribute<PersistentAttribute>(true);
                if (attribute == null)
                {
                    continue;
                }

                if (propertyInfo.GetSetMethod(true) == null || propertyInfo.GetGetMethod(true) == null)
                {
                    throw new ArgumentException(string.Format("Property {0} is missing a getter or setter", propertyInfo.Name));
                }

                yield return new PersistentProperty
                {
                    Property = propertyInfo,
                    Attribute = attribute
                };
            }
        }

        private string BasePropertyName(KeyValuePair<string, EntityProperty> kvp)
        {
            int extensionPosition = kvp.Key.LastIndexOf(this.ExtensionSuffix, StringComparison.InvariantCulture);
            return extensionPosition != -1 ?
                kvp.Key.Substring(0, extensionPosition) :
                kvp.Key;
        }

        private static KeyValuePair<string, EntityProperty> PropertyCombiner(IGrouping<string, KeyValuePair<string, EntityProperty>> group)
        {
            if (group.Count() == 1)
            {
                return group.First();
            }
            EntityProperty firstEntityProperty = group.First().Value;
            switch (firstEntityProperty.PropertyType)
            {
                case EdmType.String:
                    string stringValue = string.Join(string.Empty, group.OrderBy(SelectKey).Select(SelectStringValue));
                    return new KeyValuePair<string, EntityProperty>(group.Key, new EntityProperty(stringValue));
                case EdmType.Binary:
                    byte[] binaryValue = group.OrderBy(SelectKey).SelectMany(SelectBinaryValue).ToArray();
                    return new KeyValuePair<string, EntityProperty>(group.Key, new EntityProperty(binaryValue));
                default:
                    string error = string.Format("Can only combine binary or string properties. Property:{0} Type:{1}", group.Key, firstEntityProperty.PropertyType);
                    throw new ArgumentException(error, "group");
            }
        }

        private static string SelectKey(KeyValuePair<string, EntityProperty> kvp)
        {
            return kvp.Key;
        }

        private static EntityProperty SelectValue(KeyValuePair<string, EntityProperty> kvp)
        {
            return kvp.Value;
        }

        private static string SelectStringValue(KeyValuePair<string, EntityProperty> kvp)
        {
            return kvp.Value.StringValue;
        }

        private static byte[] SelectBinaryValue(KeyValuePair<string, EntityProperty> kvp)
        {
            return kvp.Value.BinaryValue;
        }

        private byte[] CloneBytes(byte[] src, int startOffset, int maxLength)
        {
            int length = Math.Min(src.Length - startOffset, maxLength);
            var clone = new byte[length];
            Buffer.BlockCopy(src, startOffset, clone, 0, length);
            return clone;
        }

        private void Expand(PersistentProperty persistentProperty, string value, EntityProperty storageProperty, Dictionary<string, EntityProperty> storageProperties)
        {
            // Get the first section into our normal Property
            storageProperty.StringValue = value.Substring(0, PropertyConstants.MaxStringLength);

            // Fill in the rest
            int extensionNumber = 1;
            int sectionStart = PropertyConstants.MaxStringLength;
            while (value.Length > sectionStart)
            {
                string sectionPart = sectionStart + PropertyConstants.MaxStringLength > value.Length ?
                    value.Substring(sectionStart) :
                    value.Substring(sectionStart, PropertyConstants.MaxStringLength);

                string propertyName = GetExtensionName(persistentProperty.Property.Name, extensionNumber);
                storageProperties.Add(propertyName, new EntityProperty(sectionPart));

                extensionNumber++;
                sectionStart += PropertyConstants.MaxStringLength;
            }
        }

        private void Expand(PersistentProperty persistentProperty, byte[] value, EntityProperty storageProperty, Dictionary<string, EntityProperty> storageProperties)
        {
            // Get the first section into our normal Property
            storageProperty.BinaryValue = CloneBytes(value, 0, PropertyConstants.MaxPropertySize);

            // Fill in the rest
            int extensionNumber = 1;
            int sectionStart = PropertyConstants.MaxPropertySize;
            while (value.Length > sectionStart)
            {
                byte[] sectionPart = CloneBytes(value, sectionStart, PropertyConstants.MaxPropertySize);
                string propertyName = GetExtensionName(persistentProperty.Property.Name, extensionNumber);
                storageProperties.Add(propertyName, new EntityProperty(sectionPart));

                extensionNumber++;
                sectionStart += PropertyConstants.MaxPropertySize;
            }
        }

        private string GetExtensionName(string propertyName, int extensionNumber)
        {
            string extensionPropertyName = string.Format("{0}{1}{2:000}", propertyName, this.ExtensionSuffix, extensionNumber);
            if (extensionPropertyName.Length > PropertyConstants.MaxPropertyNameLength)
            {
                throw new ArgumentException("Expanded Property name is too long (>255 char) for table entity", extensionPropertyName);
            }
            return extensionPropertyName;
        }
    }
}
