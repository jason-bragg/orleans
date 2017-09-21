using System;
using System.Reflection;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Orleans.Azure
{
    /// <summary>
    /// Property converter that serializes objects uses json.
    /// </summary>
    public class JsonPropertyConverter : IPropertyConverter
    {
        private const Formatting formatting = Formatting.None;

        private readonly JsonSerializerSettings settings = new JsonSerializerSettings
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
        };

        public void ConvertFromStorage(object obj, PropertyInfo userProperty, EntityProperty storageProperty)
        {
            string text = storageProperty.StringValue;
            object userData = GeneratePropertyFromString(text, userProperty.PropertyType);
            userProperty.SetValue(obj, userData, null);
        }

        public void ConvertToStorage(object obj, PropertyInfo userProperty, out EntityProperty storageProperty)
        {
            object userData = userProperty.GetValue(obj, null);
            string encoded = GenerateStringFromProperty(userData);
            storageProperty = EntityProperty.GeneratePropertyForString(encoded);
        }

        private object GeneratePropertyFromString(string text, Type type)
        {
            object userData = text == null ? null : JsonConvert.DeserializeObject(text, type, settings);

            return userData;
        }

        private string GenerateStringFromProperty(object userData)
        {
            string encoded = userData == null ? null : JsonConvert.SerializeObject(userData, formatting, settings);

            return encoded;
        }
    }
}
