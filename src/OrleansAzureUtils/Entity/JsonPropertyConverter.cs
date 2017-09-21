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
        private readonly JsonSerializerSettings settings;

        public JsonPropertyConverter() : this(DefaultJsonSettings.Instance){}
        
        public JsonPropertyConverter(JsonSerializerSettings settings)
        {
            this.settings = settings;
        }

        public void ConvertFromStorage(object obj, PropertyInfo userProperty, EntityProperty storageProperty)
        {
            string text = storageProperty.StringValue;
            object userData = text == null ? null : JsonConvert.DeserializeObject(text, userProperty.PropertyType, this.settings);
            userProperty.SetValue(obj, userData, null);
        }

        public void ConvertToStorage(object obj, PropertyInfo userProperty, out EntityProperty storageProperty)
        {
            object userData = userProperty.GetValue(obj, null);
            string encoded = userData == null ? null : JsonConvert.SerializeObject(userData, formatting, this.settings);
            storageProperty = EntityProperty.GeneratePropertyForString(encoded);
        }

        public class DefaultJsonSettings : JsonSerializerSettings
        {
            public static JsonSerializerSettings Instance { get; } = new DefaultJsonSettings();

            public DefaultJsonSettings()
            {
                this.DateTimeZoneHandling = DateTimeZoneHandling.Utc;
            }
        }
    }
}
