using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.WindowsAzure.Storage.Table;

namespace Orleans.Azure
{
    /// <summary>
    /// Table entity that only stores fields marked to be persisted.  It can persist any type natively
    ///  supported by table storage or any custom types added to the Property converter.
    /// </summary>
    public class EntityConverter<TPoco> : IEntityConverter<TPoco>
        where TPoco : new()
    {
        private const string DefaultExtensionSuffix = "_Expanded_";
        private readonly PropertyConverter propertyConverter;
        private readonly PropertyInfo[] properties;

        public EntityConverter()
            : this(DefaultPropertyConverter.Instance)
        {
        }

        public EntityConverter(IPropertyConverter propertyConverter)
        {
            this.propertyConverter = new PropertyConverter(propertyConverter);
            this.properties = this.BuildFilteredProperties();
        }

        /// <summary>
        /// Gets the suffix to use for overflow columns on large properties
        /// </summary>
        public string ExtensionSuffix { get; set; } = DefaultExtensionSuffix;
        
        public TPoco ConvertFromStorage(DynamicTableEntity entity)
        {
            TPoco poco = new TPoco();
            // Group and combine properties
            var combinedProperties = entity.Properties.GroupBy(this.BasePropertyName)
                                               .Select(PropertyCombiner)
                                               .ToDictionary(SelectKey, SelectValue);

            foreach (PropertyInfo persistentPropery in this.properties)
            {
                if (combinedProperties.TryGetValue(persistentPropery.Name, out EntityProperty entityProperty))
                {
                    this.propertyConverter.ConvertFromStorage(poco, persistentPropery, entityProperty);
                }
            }
            return poco;
        }


        public DynamicTableEntity ConvertToStorage(TPoco poco)
        {
            DynamicTableEntity entity = new DynamicTableEntity();

            foreach (PropertyInfo persistentProperty in this.properties)
            {
                if (persistentProperty.Name.Contains(this.ExtensionSuffix))
                {
                    throw new ArgumentException("Property names should not contain extension suffix.", persistentProperty.Name);
                }

                this.propertyConverter.ConvertToStorage(this, persistentProperty, out EntityProperty storageProperty);

                switch (storageProperty.PropertyType)
                {
                    case EdmType.String:
                        WriteStringProperty(persistentProperty, storageProperty, entity.Properties);
                        break;
                    case EdmType.Binary:
                        WriteBinaryProperty(persistentProperty, storageProperty, entity.Properties);
                        break;
                    default:
                        entity.Properties.Add(persistentProperty.Name, storageProperty);
                        break;
                }
            }

            if (entity.Properties.Count > PropertyConstants.MaxProperties)
            {
                throw new IndexOutOfRangeException("Property count exceeds maximum Azure Table Storage Row Limit of 252");
            }

            return entity;
        }

        private void WriteStringProperty(PropertyInfo persistentProperty, EntityProperty storageProperty, IDictionary<string, EntityProperty> storageProperties)
        {
            if (!string.IsNullOrEmpty(storageProperty.StringValue))
            {
                string value = storageProperty.StringValue;
                // if bit enough to expand and expandable, expand
                if (value.Length > PropertyConstants.MaxStringLength)
                {
                    // The Property needs to be split apart as it won't fit into one cell
                    this.Expand(persistentProperty, value, storageProperty, storageProperties);
                }
            }
            storageProperties.Add(persistentProperty.Name, storageProperty);
        }

        private void WriteBinaryProperty(PropertyInfo persistentProperty, EntityProperty storageProperty, IDictionary<string, EntityProperty> storageProperties)
        {
            if (storageProperty.BinaryValue != null)
            {
                byte[] value = storageProperty.BinaryValue;
                // if bit enough to expand and expandable, expand
                if (value.Length > PropertyConstants.MaxPropertySize)
                {
                    // The Property needs to be split apart as it won't fit into one cell
                    this.Expand(persistentProperty, value, storageProperty, storageProperties);
                }
            }
            storageProperties.Add(persistentProperty.Name, storageProperty);
        }

        private PropertyInfo[] BuildFilteredProperties()
        {
            return typeof(TPoco)
                .GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Where(pi => pi.GetSetMethod(true) != null)
                .Where(pi => pi.GetGetMethod(true) != null)
                .ToArray();
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

        private void Expand(PropertyInfo persistentProperty, string value, EntityProperty storageProperty, IDictionary<string, EntityProperty> storageProperties)
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

                string propertyName = GetExtensionName(persistentProperty.Name, extensionNumber);
                storageProperties.Add(propertyName, new EntityProperty(sectionPart));

                extensionNumber++;
                sectionStart += PropertyConstants.MaxStringLength;
            }
        }

        private void Expand(PropertyInfo persistentProperty, byte[] value, EntityProperty storageProperty, IDictionary<string, EntityProperty> storageProperties)
        {
            // Get the first section into our normal Property
            storageProperty.BinaryValue = CloneBytes(value, 0, PropertyConstants.MaxPropertySize);

            // Fill in the rest
            int extensionNumber = 1;
            int sectionStart = PropertyConstants.MaxPropertySize;
            while (value.Length > sectionStart)
            {
                byte[] sectionPart = CloneBytes(value, sectionStart, PropertyConstants.MaxPropertySize);
                string propertyName = GetExtensionName(persistentProperty.Name, extensionNumber);
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
