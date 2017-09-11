using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans.Runtime;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Orleans.AzureUtils
{
    public class AzureTableFactory
    {
        private readonly string connectionString;
        private readonly Logger logger;
        private readonly CloudTableClient creationClient;
        private readonly ConcurrentDictionary<string, CloudTable> tables;

        public AzureTableFactory(string connectionString, TableRequestOptions createOption, Logger logger = null)
        {
            this.connectionString = connectionString;
            this.logger = logger;
            this.creationClient = GetCloudTableClient(createOption);
            this.tables = new ConcurrentDictionary<string, CloudTable>();
        }

        public async Task<CloudTable> CreateTable(string tableName, TableRequestOptions operationOptions)
        {
            if (!tables.TryGetValue(tableName, out CloudTable table))
            {
                table = await CreateTableInternal(tableName, operationOptions);
                this.tables.TryAdd(tableName, table);
            }
            return table;
        }

        public async Task DeleteTable(string tableName)
        {
            const string operation = "DeleteTable";
            var startTime = DateTime.UtcNow;

            try
            {
                if (!tables.TryRemove(tableName, out CloudTable table))
                {
                    table = this.creationClient.GetTableReference(tableName);
                }

                bool didDelete = await table.DeleteIfExistsAsync();

                if (didDelete)
                {
                    this.logger?.Info(ErrorCode.AzureTable_03, "Deleted Azure storage table {0}", tableName);
                }
            }
            catch (Exception exc)
            {
                this.logger?.Error(ErrorCode.AzureTable_04, "Could not delete storage table {0}", exc);
                throw;
            }
            finally
            {
                CheckAlertSlowAccess(tableName, operation, startTime);
            }
        }

        private async Task<CloudTable> CreateTableInternal(string tableName, TableRequestOptions operationOptions)
        {
            const string operation = "InitTable";
            var startTime = DateTime.UtcNow;

            try
            {
                CloudTable tableRef = this.creationClient.GetTableReference(tableName);
                bool didCreate = await tableRef.CreateIfNotExistsAsync();
                
                this.logger?.Info(ErrorCode.AzureTable_01, "{0} Azure storage table {1}", (didCreate ? "Created" : "Attached to"), tableName);

                CloudTableClient tableOperationsClient = GetCloudTableClient(operationOptions);
                return tableOperationsClient.GetTableReference(tableName);
            }
            catch (Exception exc)
            {
                this.logger?.Error(ErrorCode.AzureTable_02, $"Could not initialize connection to storage table {tableName}", exc);
                throw;
            }
            finally
            {
                CheckAlertSlowAccess(tableName, operation, startTime);
            }

        }

        private CloudTableClient GetCloudTableClient(TableRequestOptions createOption)
        {
            try
            {
                CloudStorageAccount storageAccount = AzureStorageUtils.GetCloudStorageAccount(this.connectionString);
                CloudTableClient creationClient = storageAccount.CreateCloudTableClient();
                creationClient.DefaultRequestOptions = createOption;
                return creationClient;
            }
            catch (Exception exc)
            {
                this.logger?.Error(ErrorCode.AzureTable_18, "Error creating CloudTableCreationClient.", exc);
                throw;
            }
        }

        private void CheckAlertSlowAccess(string tableName, string operation, DateTime startOperation)
        {
            var timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > creationClient.DefaultRequestOptions.ServerTimeout)
            {
                this.logger?.Warn(ErrorCode.AzureTable_15, "Slow access to Azure Table {0} for {1}, which took {2}.", tableName, operation, timeSpan);
            }
        }
    }
}
