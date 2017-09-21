using System;
using System.Runtime.Serialization;
using Orleans.Runtime;
using System.Globalization;

namespace Orleans.Azure
{
    [Serializable]
    public class PropertyConversionException : OrleansException
    {
        private const string MessageFormat = "Failed to convert property.  Action: {0}, ObjectType: {1}, PropertyType: {2}, PropertyName: {3}";

        internal string Action { get; private set; }
        internal string ObjectType { get; private set; }
        internal string PropertyType { get; set; }
        internal string PropertyName { get; set; }

        public PropertyConversionException() : this("Failed to convert property") { }
        public PropertyConversionException(string message) : base(message) { }
        public PropertyConversionException(string message, Exception inner) : base(message, inner) { }

        public PropertyConversionException(string action, Type objectType, Type propertyType, string propertyName, Exception ex)
            : this(String.Format(CultureInfo.InvariantCulture, MessageFormat, action, objectType, propertyType, propertyName), ex)
        {
            Action = action;
            ObjectType = objectType.ToString();
            PropertyType = propertyType.ToString();
            PropertyName = propertyName;
        }

        public PropertyConversionException(string action, Type objectType, Type propertyType, string propertyName)
            : this(String.Format(CultureInfo.InvariantCulture, MessageFormat, action, objectType, propertyType, propertyName))
        {
            Action = action;
            ObjectType = objectType.ToString();
            PropertyType = propertyType.ToString();
            PropertyName = propertyName;
        }

        public PropertyConversionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            Action = info.GetString("Action");
            ObjectType = info.GetString("ObjectType");
            PropertyType = info.GetString("PropertyType");
            PropertyName = info.GetString("PropertyName");
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Action", Action);
            info.AddValue("ObjectType", ObjectType);
            info.AddValue("PropertyType", PropertyType);
            info.AddValue("PropertyName", PropertyName);
            base.GetObjectData(info, context);
        }

        public static PropertyConversionException CreateWriteError(Type objectType, Type propertyType,
                                                           string propertyName, Exception ex = null)
        {
            return (ex == null)
                       ? new PropertyConversionException("Write", objectType, propertyType, propertyName)
                       : new PropertyConversionException("Write", objectType, propertyType, propertyName, ex);
        }

        public static PropertyConversionException CreateReadError(Type objectType, Type propertyType,
                                                                  string propertyName, Exception ex = null)
        {
            return (ex == null)
                       ? new PropertyConversionException("Read", objectType, propertyType, propertyName)
                       : new PropertyConversionException("Read", objectType, propertyType, propertyName, ex);
        }
    }
}
