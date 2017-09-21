using System;
using System.Runtime.Serialization;
using Orleans.Runtime;
using System.Globalization;

namespace Orleans.Azure
{
    [Serializable]
    public class PropertyConversionException : OrleansException
    {
        private const string MessageFormat = "Failed to convert property.  Action: {0}, EntityType: {1}, PropertyType: {2}, PropertyName: {3}";

        internal string Action { get; private set; }
        internal string EntityType { get; private set; }
        internal string PropertyType { get; set; }
        internal string PropertyName { get; set; }

        public PropertyConversionException() : this("Failed to convert property") { }
        public PropertyConversionException(string message) : base(message) { }
        public PropertyConversionException(string message, Exception inner) : base(message, inner) { }

        public PropertyConversionException(string action, Type entityType, Type propertyType, string propertyName, Exception ex)
            : this(String.Format(CultureInfo.InvariantCulture, MessageFormat, action, entityType, propertyType, propertyName), ex)
        {
            Action = action;
            EntityType = entityType.ToString();
            PropertyType = propertyType.ToString();
            PropertyName = propertyName;
        }

        public PropertyConversionException(string action, Type entityType, Type propertyType, string propertyName)
            : this(String.Format(CultureInfo.InvariantCulture, MessageFormat, action, entityType, propertyType, propertyName))
        {
            Action = action;
            EntityType = entityType.ToString();
            PropertyType = propertyType.ToString();
            PropertyName = propertyName;
        }

        public PropertyConversionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Action = info.GetString("Action");
            EntityType = info.GetString("EntityType");
            PropertyType = info.GetString("PropertyType");
            PropertyName = info.GetString("PropertyName");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Action", Action);
            info.AddValue("EntityType", EntityType);
            info.AddValue("PropertyType", PropertyType);
            info.AddValue("PropertyName", PropertyName);
            base.GetObjectData(info, context);
        }

        public static PropertyConversionException CreateWriteError(Type entityType, Type propertyType,
                                                           string propertyName, Exception ex = null)
        {
            return (ex == null)
                       ? new PropertyConversionException("Write", entityType, propertyType, propertyName)
                       : new PropertyConversionException("Write", entityType, propertyType, propertyName, ex);
        }

        public static PropertyConversionException CreateReadError(Type entityType, Type propertyType,
                                                                  string propertyName, Exception ex = null)
        {
            return (ex == null)
                       ? new PropertyConversionException("Read", entityType, propertyType, propertyName)
                       : new PropertyConversionException("Read", entityType, propertyType, propertyName, ex);
        }
    }
}
