using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.AzureUtils;
using Orleans.Providers;
using Orleans.Providers.Azure;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;
using Orleans.Azure;

namespace Orleans.Storage
{
    /// <summary>
    /// Simple storage provider for writing grain state data to Azure table storage.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Required configuration params: <c>DataConnectionString</c>
    /// </para>
    /// <para>
    /// Optional configuration params:
    /// <c>TableName</c> -- defaults to <c>OrleansGrainState</c>
    /// <c>DeleteStateOnClear</c> -- defaults to <c>false</c>
    /// </para>
    /// </remarks>
    /// <example>
    /// Example configuration for this storage provider in OrleansConfiguration.xml file:
    /// <code>
    /// &lt;OrleansConfiguration xmlns="urn:orleans">
    ///   &lt;Globals>
    ///     &lt;StorageProviders>
    ///       &lt;Provider Type="Orleans.Storage.AzureTableStorage" Name="AzureStore"
    ///         DataConnectionString="UseDevelopmentStorage=true"
    ///         DeleteStateOnClear="true"
    ///       />
    ///   &lt;/StorageProviders>
    /// </code>
    /// </example>
    public class AzureTableStorage : IStorageProvider, IRestExceptionDecoder
    {
        internal const string DataConnectionStringPropertyName = "DataConnectionString";
        internal const string TableNamePropertyName = "TableName";
        internal const string DeleteOnClearPropertyName = "DeleteStateOnClear";
        internal const string UseJsonFormatPropertyName = "UseJsonFormat";
        internal const string TableNameDefaultValue = "OrleansGrainState";
        internal const string UseLegacyStorageFormatName = "UseLegacyStorageFormat";
        private string dataConnectionString;
        private string tableName;
        private string serviceId;
        private GrainStateTableDataManager tableDataManager;
        private bool isDeleteStateOnClear;
        private static int counter;
        private readonly int id;

        private bool useJsonFormat;
        private Newtonsoft.Json.JsonSerializerSettings jsonSettings;
        private SerializationManager serializationManager;

        private IEntityConverter<object> converter;

        // for test
        public IEntityConverter<object> Converter => converter;

        /// <summary> Name of this storage provider instance. </summary>
        /// <see cref="IProvider.Name"/>
        public string Name { get; private set; }

        /// <summary> Logger used by this storage provider instance. </summary>
        /// <see cref="IStorageProvider.Log"/>
        public Logger Log { get; private set; }

        /// <summary> Default constructor </summary>
        public AzureTableStorage()
        {
            tableName = TableNameDefaultValue;
            id = Interlocked.Increment(ref counter);
        }

        /// <summary> Initialization function for this storage provider. </summary>
        /// <see cref="IProvider.Init"/>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Name = name;
            serviceId = providerRuntime.ServiceId.ToString();
            this.serializationManager = providerRuntime.ServiceProvider.GetRequiredService<SerializationManager>();

            if (!config.Properties.ContainsKey(DataConnectionStringPropertyName) || string.IsNullOrWhiteSpace(config.Properties[DataConnectionStringPropertyName]))
                throw new ArgumentException("DataConnectionString property not set");

            dataConnectionString = config.Properties["DataConnectionString"];

            if (config.Properties.ContainsKey(TableNamePropertyName))
                tableName = config.Properties[TableNamePropertyName];

            isDeleteStateOnClear = config.Properties.ContainsKey(DeleteOnClearPropertyName) &&
                "true".Equals(config.Properties[DeleteOnClearPropertyName], StringComparison.OrdinalIgnoreCase);

            Log = providerRuntime.GetLogger("Storage.AzureTableStorage." + id);

            var initMsg = string.Format("Init: Name={0} ServiceId={1} Table={2} DeleteStateOnClear={3}",
                Name, serviceId, tableName, isDeleteStateOnClear);

            if (config.Properties.ContainsKey(UseJsonFormatPropertyName))
                useJsonFormat = "true".Equals(config.Properties[UseJsonFormatPropertyName], StringComparison.OrdinalIgnoreCase);

            var grainFactory = providerRuntime.ServiceProvider.GetRequiredService<IGrainFactory>();
            this.jsonSettings = OrleansJsonSerializer.UpdateSerializerSettings(OrleansJsonSerializer.GetDefaultSerializerSettings(this.serializationManager, grainFactory), config);
            initMsg = String.Format("{0} UseJsonFormat={1}", initMsg, useJsonFormat);

            this.converter = (config.GetBoolProperty(UseLegacyStorageFormatName, false))
                ? (IEntityConverter<object>)new LegacyEntityConverter(this.serializationManager, this.jsonSettings, this.useJsonFormat, this.Log)
                : (IEntityConverter<object>)new EntityConverter<object>(new JsonPropertyConverter(this.jsonSettings));

            Log.Info((int)AzureProviderErrorCode.AzureTableProvider_InitProvider, initMsg);
            Log.Info((int)AzureProviderErrorCode.AzureTableProvider_ParamConnectionString, "AzureTableStorage Provider is using DataConnectionString: {0}", ConfigUtilities.RedactConnectionStringInfo(dataConnectionString));
            tableDataManager = new GrainStateTableDataManager(tableName, dataConnectionString, Log);
            return tableDataManager.InitTableAsync();
        }

        // Internal method to initialize for testing
        internal void InitLogger(Logger logger)
        {
            Log = logger;
        }

        /// <summary> Shutdown this storage provider. </summary>
        /// <see cref="IProvider.Close"/>
        public Task Close()
        {
            tableDataManager = null;
            return Task.CompletedTask;
        }

        /// <summary> Read state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider.ReadStateAsync"/>
        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (tableDataManager == null) throw new ArgumentException("GrainState-Table property not initialized");

            string pk = GetKeyString(grainReference);
            if (Log.IsVerbose3) Log.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_ReadingData, "Reading: GrainType={0} Pk={1} Grainid={2} from Table={3}", grainType, pk, grainReference, tableName);
            string partitionKey = pk;
            string rowKey = grainType;
            GrainStateRecord record = await tableDataManager.Read(partitionKey, rowKey).ConfigureAwait(false);
            if (record != null)
            {
                var entity = record.Entity;
                if (entity != null)
                {
                    var loadedState = this.converter.ConvertFromStorage(entity);
                    grainState.State = loadedState ?? Activator.CreateInstance(grainState.State.GetType());
                    grainState.ETag = record.ETag;
                }
            }

            // Else leave grainState in previous default condition
        }

        /// <summary> Write state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider.WriteStateAsync"/>
        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (tableDataManager == null) throw new ArgumentException("GrainState-Table property not initialized");

            string pk = GetKeyString(grainReference);
            if (Log.IsVerbose3)
                Log.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_WritingData, "Writing: GrainType={0} Pk={1} Grainid={2} ETag={3} to Table={4}", grainType, pk, grainReference, grainState.ETag, tableName);

            DynamicTableEntity entity = this.converter.ConvertToStorage(grainState.State);
            entity.PartitionKey = pk;
            entity.RowKey = grainType;
            var record = new GrainStateRecord { Entity = entity, ETag = grainState.ETag };
            try
            {
                await DoOptimisticUpdate(() => tableDataManager.Write(record), grainType, grainReference, tableName, grainState.ETag).ConfigureAwait(false);
                grainState.ETag = record.ETag;
            }
            catch (Exception exc)
            {
                Log.Error((int)AzureProviderErrorCode.AzureTableProvider_WriteError,
                    $"Error Writing: GrainType={grainType} Grainid={grainReference} ETag={grainState.ETag} to Table={tableName} Exception={exc.Message}", exc);
                throw;
            }
        }

        /// <summary> Clear / Delete state data function for this storage provider. </summary>
        /// <remarks>
        /// If the <c>DeleteStateOnClear</c> is set to <c>true</c> then the table row
        /// for this grain will be deleted / removed, otherwise the table row will be
        /// cleared by overwriting with default / null values.
        /// </remarks>
        /// <see cref="IStorageProvider.ClearStateAsync"/>
        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (tableDataManager == null) throw new ArgumentException("GrainState-Table property not initialized");

            string pk = GetKeyString(grainReference);
            if (Log.IsVerbose3) Log.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_WritingData, "Clearing: GrainType={0} Pk={1} Grainid={2} ETag={3} DeleteStateOnClear={4} from Table={5}", grainType, pk, grainReference, grainState.ETag, isDeleteStateOnClear, tableName);
            var entity = new DynamicTableEntity(pk, grainType);
            var record = new GrainStateRecord { Entity = entity, ETag = grainState.ETag };
            string operation = "Clearing";
            try
            {
                if (isDeleteStateOnClear)
                {
                    operation = "Deleting";
                    await DoOptimisticUpdate(() => tableDataManager.Delete(record), grainType, grainReference, tableName, grainState.ETag).ConfigureAwait(false);
                }
                else
                {
                    await DoOptimisticUpdate(() => tableDataManager.Write(record), grainType, grainReference, tableName, grainState.ETag).ConfigureAwait(false);
                }

                grainState.ETag = record.ETag; // Update in-memory data to the new ETag
            }
            catch (Exception exc)
            {
                Log.Error((int)AzureProviderErrorCode.AzureTableProvider_DeleteError, string.Format("Error {0}: GrainType={1} Grainid={2} ETag={3} from Table={4} Exception={5}",
                    operation, grainType, grainReference, grainState.ETag, tableName, exc.Message), exc);
                throw;
            }
        }

        private static async Task DoOptimisticUpdate(Func<Task> updateOperation, string grainType, GrainReference grainReference, string tableName, string currentETag)
        {
            try
            {
                await updateOperation.Invoke().ConfigureAwait(false);
            }
            catch (StorageException ex) when (ex.IsPreconditionFailed() || ex.IsConflict())
            {
                throw new TableStorageUpdateConditionNotSatisfiedException(grainType, grainReference, tableName, "Unknown", currentETag, ex);
            }
        }

        private string GetKeyString(GrainReference grainReference)
        {
            var key = String.Format("{0}_{1}", serviceId, grainReference.ToKeyString());
            return AzureStorageUtils.SanitizeTableProperty(key);
        }

        internal class GrainStateRecord        {
            public string ETag { get; set; }
            public DynamicTableEntity Entity { get; set; }
        }

        private class GrainStateTableDataManager
        {
            public string TableName { get; private set; }
            private readonly AzureTableDataManager<DynamicTableEntity> tableManager;
            private readonly Logger logger;

            public GrainStateTableDataManager(string tableName, string storageConnectionString, Logger logger)
            {
                this.logger = logger;
                TableName = tableName;
                tableManager = new AzureTableDataManager<DynamicTableEntity>(tableName, storageConnectionString);
            }

            public Task InitTableAsync()
            {
                return tableManager.InitTableAsync();
            }

            public async Task<GrainStateRecord> Read(string partitionKey, string rowKey)
            {
                if (logger.IsVerbose3) logger.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_Storage_Reading, "Reading: PartitionKey={0} RowKey={1} from Table={2}", partitionKey, rowKey, TableName);
                try
                {
                    Tuple<DynamicTableEntity, string> data = await tableManager.ReadSingleTableEntryAsync(partitionKey, rowKey).ConfigureAwait(false);
                    if (data == null || data.Item1 == null)
                    {
                        if (logger.IsVerbose2) logger.Verbose2((int)AzureProviderErrorCode.AzureTableProvider_DataNotFound, "DataNotFound reading: PartitionKey={0} RowKey={1} from Table={2}", partitionKey, rowKey, TableName);
                        return null;
                    }
                    DynamicTableEntity stateEntity = data.Item1;
                    var record = new GrainStateRecord { Entity = stateEntity, ETag = data.Item2 };
                    if (logger.IsVerbose3) logger.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_Storage_DataRead, "Read: PartitionKey={0} RowKey={1} from Table={2} with ETag={3}", stateEntity.PartitionKey, stateEntity.RowKey, TableName, record.ETag);
                    return record;
                }
                catch (Exception exc)
                {
                    if (AzureStorageUtils.TableStorageDataNotFound(exc))
                    {
                        if (logger.IsVerbose2) logger.Verbose2((int)AzureProviderErrorCode.AzureTableProvider_DataNotFound, "DataNotFound reading (exception): PartitionKey={0} RowKey={1} from Table={2} Exception={3}", partitionKey, rowKey, TableName, LogFormatter.PrintException(exc));
                        return null;  // No data
                    }
                    throw;
                }
            }

            public async Task Write(GrainStateRecord record)
            {
                var entity = record.Entity;
                if (logger.IsVerbose3) logger.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_Storage_Writing, "Writing: PartitionKey={0} RowKey={1} to Table={2} with ETag={3}", entity.PartitionKey, entity.RowKey, TableName, record.ETag);
                string eTag = String.IsNullOrEmpty(record.ETag) ?
                    await tableManager.CreateTableEntryAsync(entity).ConfigureAwait(false) :
                    await tableManager.UpdateTableEntryAsync(entity, record.ETag).ConfigureAwait(false);
                record.ETag = eTag;
            }

            public async Task Delete(GrainStateRecord record)
            {
                var entity = record.Entity;
                if (logger.IsVerbose3) logger.Verbose3((int)AzureProviderErrorCode.AzureTableProvider_Storage_Writing, "Deleting: PartitionKey={0} RowKey={1} from Table={2} with ETag={3}", entity.PartitionKey, entity.RowKey, TableName, record.ETag);
                await tableManager.DeleteTableEntryAsync(entity, record.ETag).ConfigureAwait(false);
                record.ETag = null;
            }
        }

        /// <summary> Decodes Storage exceptions.</summary>
        public bool DecodeException(Exception e, out HttpStatusCode httpStatusCode, out string restStatus, bool getRESTErrors = false)
        {
            return AzureStorageUtils.EvaluateException(e, out httpStatusCode, out restStatus, getRESTErrors);
        }
    }
}