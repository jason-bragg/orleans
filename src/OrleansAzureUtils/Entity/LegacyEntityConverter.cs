using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Orleans.Serialization;
using Orleans.Runtime;
using System.Linq;

namespace Orleans.Azure
{
    public class LegacyEntityConverter : IEntityConverter<object>
    {
        private readonly SerializationManager serializationManager;
        private readonly JsonSerializerSettings settings;
        private readonly bool useJsonFormat;
        private readonly Logger Log;

        private const int MAX_DATA_CHUNKS_COUNT = 15;

        private const string BINARY_DATA_PROPERTY_NAME = "Data";
        private const string STRING_DATA_PROPERTY_NAME = "StringData";

        public LegacyEntityConverter(SerializationManager serializationManager, JsonSerializerSettings settings, bool useJsonFormat, Logger log)
        {
            this.serializationManager = serializationManager;
            this.settings = settings;
            this.useJsonFormat = useJsonFormat;
            this.Log = log;
        }

        public object ConvertFromStorage(DynamicTableEntity entity)
        {
            var binaryData = ReadBinaryData(entity);
            var stringData = ReadStringData(entity);

            object dataValue = null;
            try
            {
                if (binaryData.Length > 0)
                {
                    // Rehydrate
                    dataValue = this.serializationManager.DeserializeFromByteArray<object>(binaryData);
                }
                else if (!string.IsNullOrEmpty(stringData))
                {
                    dataValue = JsonConvert.DeserializeObject<object>(stringData, this.settings);
                }

                // Else, no data found
            }
            catch (Exception exc)
            {
                var sb = new StringBuilder();
                if (binaryData.Length > 0)
                {
                    sb.AppendFormat("Unable to convert from storage format GrainStateEntity.Data={0}", binaryData);
                }
                else if (!string.IsNullOrEmpty(stringData))
                {
                    sb.AppendFormat("Unable to convert from storage format GrainStateEntity.StringData={0}", stringData);
                }
                if (dataValue != null)
                {
                    sb.AppendFormat("Data Value={0} Type={1}", dataValue, dataValue.GetType());
                }

                Log.Error(0, sb.ToString(), exc);
                throw new AggregateException(sb.ToString(), exc);
            }

            return dataValue;
        }
        
        public DynamicTableEntity ConvertToStorage(object poco)
        {
            int dataSize;
            IEnumerable<EntityProperty> properties;
            string basePropertyName;
            var entity = new DynamicTableEntity();

            if (useJsonFormat)
            {
                // http://james.newtonking.com/json/help/index.html?topic=html/T_Newtonsoft_Json_JsonConvert.htm
                string data = Newtonsoft.Json.JsonConvert.SerializeObject(poco, this.settings);

                if (Log.IsVerbose3) Log.Verbose3("Writing JSON data size = {0} for grain id = Partition={1} / Row={2}",
                    data.Length, entity.PartitionKey, entity.RowKey);

                // each Unicode character takes 2 bytes
                dataSize = data.Length * 2;

                properties = SplitStringData(data).Select(t => new EntityProperty(t));
                basePropertyName = STRING_DATA_PROPERTY_NAME;
            }
            else
            {
                // Convert to binary format

                byte[] data = this.serializationManager.SerializeToByteArray(poco);

                if (Log.IsVerbose3) Log.Verbose3("Writing binary data size = {0} for grain id = Partition={1} / Row={2}",
                    data.Length, entity.PartitionKey, entity.RowKey);

                dataSize = data.Length;

                properties = SplitBinaryData(data).Select(t => new EntityProperty(t));
                basePropertyName = BINARY_DATA_PROPERTY_NAME;
            }

            CheckMaxDataSize(dataSize, PropertyConstants.MaxPropertySize * MAX_DATA_CHUNKS_COUNT);

            foreach (var keyValuePair in properties.Zip(GetPropertyNames(basePropertyName),
                (property, name) => new KeyValuePair<string, EntityProperty>(name, property)))
            {
                entity.Properties.Add(keyValuePair);
            }
            return entity;
        }

        private static IEnumerable<byte[]> ReadBinaryDataChunks(DynamicTableEntity entity)
        {
            foreach (var binaryDataPropertyName in GetPropertyNames(BINARY_DATA_PROPERTY_NAME))
            {
                EntityProperty dataProperty;
                if (entity.Properties.TryGetValue(binaryDataPropertyName, out dataProperty))
                {
                    switch (dataProperty.PropertyType)
                    {
                        // if TablePayloadFormat.JsonNoMetadata is used
                        case EdmType.String:
                            var stringValue = dataProperty.StringValue;
                            if (!string.IsNullOrEmpty(stringValue))
                            {
                                yield return Convert.FromBase64String(stringValue);
                            }
                            break;

                        // if any payload type providing metadata is used
                        case EdmType.Binary:
                            var binaryValue = dataProperty.BinaryValue;
                            if (binaryValue != null && binaryValue.Length > 0)
                            {
                                yield return binaryValue;
                            }
                            break;
                    }
                }
            }
        }

        private static byte[] ReadBinaryData(DynamicTableEntity entity)
        {
            var dataChunks = ReadBinaryDataChunks(entity).ToArray();
            var dataSize = dataChunks.Select(d => d.Length).Sum();
            var result = new byte[dataSize];
            var startIndex = 0;
            foreach (var dataChunk in dataChunks)
            {
                Array.Copy(dataChunk, 0, result, startIndex, dataChunk.Length);
                startIndex += dataChunk.Length;
            }
            return result;
        }

        private static IEnumerable<string> ReadStringDataChunks(DynamicTableEntity entity)
        {
            foreach (var stringDataPropertyName in GetPropertyNames(STRING_DATA_PROPERTY_NAME))
            {
                EntityProperty dataProperty;
                if (entity.Properties.TryGetValue(stringDataPropertyName, out dataProperty))
                {
                    var data = dataProperty.StringValue;
                    if (!string.IsNullOrEmpty(data))
                    {
                        yield return data;
                    }
                }
            }
        }

        private static string ReadStringData(DynamicTableEntity entity)
        {
            return string.Join(string.Empty, ReadStringDataChunks(entity));
        }

        private static IEnumerable<string> GetPropertyNames(string basePropertyName)
        {
            yield return basePropertyName;
            for (var i = 1; i < MAX_DATA_CHUNKS_COUNT; ++i)
            {
                yield return basePropertyName + i;
            }
        }

        private static IEnumerable<string> SplitStringData(string stringData)
        {
            var startIndex = 0;
            while (startIndex < stringData.Length)
            {
                var chunkSize = Math.Min(PropertyConstants.MaxStringLength, stringData.Length - startIndex);

                yield return stringData.Substring(startIndex, chunkSize);

                startIndex += chunkSize;
            }
        }

        private static IEnumerable<byte[]> SplitBinaryData(byte[] binaryData)
        {
            var startIndex = 0;
            while (startIndex < binaryData.Length)
            {
                var chunkSize = Math.Min(PropertyConstants.MaxPropertySize, binaryData.Length - startIndex);

                var chunk = new byte[chunkSize];
                Array.Copy(binaryData, startIndex, chunk, 0, chunkSize);
                yield return chunk;

                startIndex += chunkSize;
            }
        }
    
        private void CheckMaxDataSize(int dataSize, int maxDataSize)
        {
            if (dataSize > maxDataSize)
            {
                var msg = string.Format("Data too large to write to Azure table. Size={0} MaxSize={1}", dataSize, maxDataSize);
                Log.Error(0, msg);
                throw new ArgumentOutOfRangeException("GrainState.Size", msg);
            }
        }
    }
}
