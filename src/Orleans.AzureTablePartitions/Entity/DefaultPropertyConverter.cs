using System;
using System.Globalization;
using System.Reflection;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureTablePartitions.Abstractions;

namespace Orleans.AzureTablePartitions
{
    /// <summary>
    /// Converts various types of properties, both natively supported by table storage and custom defined types,
    /// to/from storage properties.  For the native types, the native storage properties are used.  Custom types
    /// all convert to/from string storage properties using json format.
    /// </summary>
    public class DefaultPropertyConverter : IPropertyConverter
    {
        public static IPropertyConverter Instance { get; } = new DefaultPropertyConverter();

        public void ConvertFromStorage(object obj, PropertyInfo userProperty, EntityProperty storageProperty)
        {
            try
            {
                switch (storageProperty.PropertyType)
                {
                    case EdmType.String:
                        if (userProperty.PropertyType == typeof(string) || userProperty.PropertyType == typeof(String))
                        {
                            userProperty.SetValue(obj, storageProperty.StringValue, null);
                        }
                        else
                        {
                            JsonPropertyConverter.ConvertFromStorage(obj, userProperty, storageProperty);
                        }
                        return;
                    case EdmType.Binary:
                        if (userProperty.PropertyType == typeof(byte[]))
                        {
                            userProperty.SetValue(obj, storageProperty.BinaryValue, null);
                            return;
                        }
                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                    case EdmType.Boolean:
                        if (userProperty.PropertyType == typeof(bool) || userProperty.PropertyType == typeof(bool?))
                        {
                            userProperty.SetValue(obj, storageProperty.BooleanValue, null);
                            return;
                        }
                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                    case EdmType.DateTime:
                        if (userProperty.PropertyType == typeof(DateTime) &&
                            storageProperty.DateTime != null)
                        {
                            userProperty.SetValue(obj, storageProperty.DateTime.Value, null);
                            return;
                        }
                        if (userProperty.PropertyType == typeof(DateTime?))
                        {
                            userProperty.SetValue(obj, storageProperty.DateTime, null);
                            return;
                        }
                        if (userProperty.PropertyType == typeof(DateTimeOffset) &&
                            storageProperty.DateTimeOffsetValue != null)
                        {
                            userProperty.SetValue(obj, storageProperty.DateTimeOffsetValue.Value, null);
                            return;
                        }
                        if (userProperty.PropertyType == typeof(DateTimeOffset?))
                        {
                            userProperty.SetValue(obj, storageProperty.DateTimeOffsetValue, null);
                            return;
                        }
                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                    case EdmType.Double:
                        if (userProperty.PropertyType == typeof(double) || userProperty.PropertyType == typeof(double?))
                        {
                            userProperty.SetValue(obj, storageProperty.DoubleValue, null);
                            return;
                        }
                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                    case EdmType.Guid:
                        if (userProperty.PropertyType == typeof(Guid) || userProperty.PropertyType == typeof(Guid?))
                        {
                            userProperty.SetValue(obj, storageProperty.GuidValue, null);
                            return;
                        }
                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                    case EdmType.Int32:
                        if (userProperty.PropertyType == typeof(int) || userProperty.PropertyType == typeof(int?))
                        {
                            userProperty.SetValue(obj, storageProperty.Int32Value, null);
                            return;
                        }
                        if (userProperty.PropertyType == typeof(uint) || userProperty.PropertyType == typeof(uint?))
                        {
                            if (storageProperty.Int32Value.HasValue)
                            {
                                userProperty.SetValue(obj, (uint)storageProperty.Int32Value, null);
                            }
                            return;
                        }

                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                    case EdmType.Int64:
                        if (userProperty.PropertyType == typeof(long) || userProperty.PropertyType == typeof(long?))
                        {
                            userProperty.SetValue(obj, storageProperty.Int64Value, null);
                            return;
                        }
                        if (userProperty.PropertyType == typeof(ulong) || userProperty.PropertyType == typeof(ulong?))
                        {
                            if (storageProperty.Int64Value.HasValue)
                            {
                                userProperty.SetValue(obj, (ulong)storageProperty.Int64Value, null);
                            }
                            return;
                        }

                        throw new ArgumentException($"Property {userProperty.Name} is the wrong type.  Entity type {userProperty.PropertyType}.  Type in data store {storageProperty.PropertyType}");
                }
                throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "Property {0} of supported type.  Entity type {1}.  Type in data store {2}", userProperty.Name, userProperty.PropertyType, storageProperty.PropertyType));
            }
            catch (Exception ex)
            {
                throw PropertyConversionException.CreateReadError(obj.GetType(), userProperty.PropertyType, userProperty.Name, ex);
            }
        }

        public void ConvertToStorage(object obj, PropertyInfo userProperty, out EntityProperty storageProperty)
        {
            try
            {
                storageProperty = null;
                object value = userProperty.GetValue(obj, null);
                if (userProperty.PropertyType == typeof(string))
                {
                    storageProperty = new EntityProperty((string)value);
                }
                else if (userProperty.PropertyType == typeof(byte[]))
                {
                    storageProperty = new EntityProperty((byte[])value);
                }
                else if (userProperty.PropertyType == typeof(int) || userProperty.PropertyType == typeof(int?))
                {
                    storageProperty = new EntityProperty((int?)value);
                }
                else if (userProperty.PropertyType == typeof(uint) || userProperty.PropertyType == typeof(uint?))
                {
                    storageProperty = new EntityProperty((int?)(uint?)value);
                }
                else if (userProperty.PropertyType == typeof(bool) || userProperty.PropertyType == typeof(bool?))
                {
                    storageProperty = new EntityProperty((bool?)value);
                }
                else if (userProperty.PropertyType == typeof(double) || userProperty.PropertyType == typeof(double?))
                {
                    storageProperty = new EntityProperty((double?)value);
                }
                else if (userProperty.PropertyType == typeof(long) || userProperty.PropertyType == typeof(long?))
                {
                    storageProperty = new EntityProperty((long?)value);
                }
                else if (userProperty.PropertyType == typeof(ulong) || userProperty.PropertyType == typeof(ulong?))
                {
                    storageProperty = new EntityProperty((long?)(ulong?)value);
                }
                else if (userProperty.PropertyType == typeof(Guid) || userProperty.PropertyType == typeof(Guid?))
                {
                    storageProperty = new EntityProperty((Guid?)value);
                }
                else if (userProperty.PropertyType == typeof(DateTime) || userProperty.PropertyType == typeof(DateTime?))
                {
                    storageProperty = new EntityProperty((DateTime?)value);
                }
                else if (userProperty.PropertyType == typeof(DateTimeOffset) || userProperty.PropertyType == typeof(DateTimeOffset?))
                {
                    storageProperty = new EntityProperty((DateTimeOffset?)value);
                }
                else if (value == null)
                {
                    storageProperty = EntityProperty.GeneratePropertyForString(null);
                }
                else
                {
                    JsonPropertyConverter.ConvertToStorage(obj, userProperty, out storageProperty);
                }
            }
            catch (Exception ex)
            {
                throw PropertyConversionException.CreateWriteError(obj.GetType(), userProperty.PropertyType, userProperty.Name, ex);
            }
        }
    }
}
