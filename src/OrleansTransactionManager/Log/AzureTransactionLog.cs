﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Orleans;
using Orleans.Serialization;
using CloudStorageAccount = Microsoft.WindowsAzure.Storage.CloudStorageAccount;

namespace Orleans.Transactions
{
    internal class AzureTransactionLog : TransactionLog
    {
        private const int CommitRecordsPerRow = 10;
        private const long CommitPartitionKey = 0;
        private const long StartPartitionKey = 1;

        private LogMode mode;
        private bool clearOnInitialize = false;
        private long logSequenceNumber = 1;
        private long startedTransactionsCount = 0;

        // Azure Tables objects for persistent storage
        private string tableName;
        private CloudStorageAccount storageAccount;
        private CloudTableClient azTableClient;

        // Log iteration indexes
        private TableQuerySegment<CommitRow> currentLogQuerySegment;
        private int rowInCurrentSegment;
        private List<CommitRecord> currentRowTransactions;
        private int recordInCurrentRow;
        private TableContinuationToken continuationToken;


        public AzureTransactionLog(string connectionString, string tableName, bool clear = false)
        {
            // Retrieve the storage account from the connection string.
            storageAccount = CloudStorageAccount.Parse(connectionString);

            // Create the table client.
            azTableClient = storageAccount.CreateCloudTableClient();
            this.tableName = tableName;

            mode = LogMode.Uninitialized;
            clearOnInitialize = clear;
        }

        public override void Initialize()
        {
            CloudTable table = azTableClient.GetTableReference(tableName);

            if (clearOnInitialize)
            {
                table.DeleteIfExists();
                Thread.Sleep(1000);
            }

            table.CreateIfNotExists();

            TableQuery<StartRow> query =
                new TableQuery<StartRow>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "0"));
            var continuationToken = new TableContinuationToken();
            var result = table.ExecuteQuerySegmented(query, continuationToken);
            
            if (result.Results.Count == 0)
            {
                // This is a fresh deployment, the StartRecord isn't created yet.
                // Create it here.
                var row = new StartRow(0);
                var operation = TableOperation.Insert(row);

                table.Execute(operation);
            }
            else
            {
                startedTransactionsCount = result.Results[0].TransactionCount;
            }

            mode = LogMode.RecoveryMode;
        }

        public async override Task<CommitRecord> GetFirstCommitRecord()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);

            continuationToken = new TableContinuationToken();

            await GetLogsFromTable("0");

            if (currentLogQuerySegment.Results.Count == 0)
            {
                // The log has no log entries
                currentLogQuerySegment = null;
                return null;
            }

            currentRowTransactions = DeserializeCommitRecords(currentLogQuerySegment.Results[0].Transactions);

            // TODO: Assert not empty?

            logSequenceNumber = currentRowTransactions[recordInCurrentRow].LSN + 1;
            return currentRowTransactions[recordInCurrentRow++];
        }

        public override async Task<CommitRecord> GetNextCommitRecord()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);

            if (currentLogQuerySegment == null)
            {
                return null;
            }

            if (recordInCurrentRow == currentRowTransactions.Count)
            {
                rowInCurrentSegment++;
                recordInCurrentRow = 0;
                currentRowTransactions = null;
            }

            if (rowInCurrentSegment == currentLogQuerySegment.Results.Count)
            {
                // No more rows in our current segment, retrieve the next segment from the Table.
                if (continuationToken.NextRowKey == null)
                {
                    currentLogQuerySegment = null;
                    return null;
                }

                await GetLogsFromTable(continuationToken.NextRowKey);
            }

            if (currentRowTransactions == null)
            {
                // TODO: assert recordInCurrentRow = 0?
                currentRowTransactions = DeserializeCommitRecords(currentLogQuerySegment.Results[rowInCurrentSegment].Transactions);
            }

            logSequenceNumber++;
            return currentRowTransactions[recordInCurrentRow++];
        }

        public override void EndRecovery()
        {
            ThrowIfNotInMode(LogMode.RecoveryMode);
            mode = LogMode.AppendMode;
        }

        public override long GetStartRecord()
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            return startedTransactionsCount;
        }

        public override async Task UpdateStartRecord(long transactionCount)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            if (transactionCount > startedTransactionsCount)
            {
                CloudTable table = azTableClient.GetTableReference(tableName);
                var op = TableOperation.Replace(new StartRow(transactionCount));
                await table.ExecuteAsync(op);
                startedTransactionsCount = transactionCount;
            }

        }

        public override async Task Append(List<CommitRecord> transactions)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            CloudTable table = azTableClient.GetTableReference(tableName);
            var batchOperation = new TableBatchOperation();

            for (int nextRecord = 0; nextRecord < transactions.Count; nextRecord += CommitRecordsPerRow)
            {
                var recordCount = Math.Min(transactions.Count - nextRecord, CommitRecordsPerRow);
                var rowTransactions = transactions.GetRange(nextRecord, recordCount);
                var row = new CommitRow(logSequenceNumber);
                foreach (var rec in rowTransactions)
                {
                    rec.LSN = logSequenceNumber++;
                }

                row.Transactions = SerializeCommitRecords(rowTransactions);
                batchOperation.Insert(row);

                if (batchOperation.Count == 100)
                {
                    await table.ExecuteBatchAsync(batchOperation);
                    batchOperation = new TableBatchOperation();
                }
            }

            if (batchOperation.Count > 0)
            {
                await table.ExecuteBatchAsync(batchOperation);
            }
        }


        public override async Task TruncateLog(long LSN)
        {
            ThrowIfNotInMode(LogMode.AppendMode);

            CloudTable table = azTableClient.GetTableReference(tableName);

            TableQuery<CommitRow> query =
                new TableQuery<CommitRow>().Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CommitPartitionKey.ToString()),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, LSN.ToString())));
            var logSegment = await table.ExecuteQuerySegmentedAsync(query, continuationToken);

            var batchOperation = new TableBatchOperation();

            while (logSegment.Results.Count > 0)
            {
                foreach (var row in logSegment)
                {
                    List<CommitRecord> transactions = DeserializeCommitRecords(row.Transactions);
                    if (transactions[transactions.Count - 1].LSN < LSN)
                    {
                        batchOperation.Delete(row);
                        if (batchOperation.Count == 100)
                        {
                            // Azure has a limit of 100 operations per batch
                            await table.ExecuteBatchAsync(batchOperation);
                            batchOperation = new TableBatchOperation();
                        }
                    }
                    else
                    {
                        if (batchOperation.Count > 0)
                        {
                            await table.ExecuteBatchAsync(batchOperation);
                        }
                        return;
                    }
                }

                logSegment = await table.ExecuteQuerySegmentedAsync(query, continuationToken);
            }

            if (batchOperation.Count > 0)
            {
                await table.ExecuteBatchAsync(batchOperation);
            }
        }

        private async Task GetLogsFromTable(string keyLowerBound)
        {
            CloudTable table = azTableClient.GetTableReference(tableName);

            TableQuery<CommitRow> query =
                new TableQuery<CommitRow>().Where(TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, CommitPartitionKey.ToString()),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, keyLowerBound)));
            currentLogQuerySegment = await table.ExecuteQuerySegmentedAsync(query, continuationToken);

            // reset the indexes
            rowInCurrentSegment = 0;
            recordInCurrentRow = 0;
        }

        private void ThrowIfNotInMode(LogMode mode)
        {
            if (this.mode != mode)
                throw new InvalidOperationException("Log has to be in {0}" + mode.ToString());
        }

        private class CommitRow : TableEntity
        {
            public CommitRow(long firstLSN)
            {
                this.PartitionKey = CommitPartitionKey.ToString(); // all entities are in the same partition for atomic read/writes
                this.RowKey = firstLSN.ToString();
            }

            public CommitRow()
            {
            }

            public string Transactions { get; set; }
        }

        private class StartRow : TableEntity
        {
            public StartRow(long transactionCount)
            {
                // only row in the table with this partition key
                this.PartitionKey = StartPartitionKey.ToString();
                this.RowKey = "0";
                base.ETag = "*";
                TransactionCount = transactionCount;
            }

            public StartRow()
            {
            }

            public long TransactionCount { get; set; }
        }

        private string SerializeCommitRecords(List<CommitRecord> records)
        {
            var serializableList = new List<Tuple<long, long, HashSet<ITransactionalGrain>>>();

            foreach (var r in records)
            {
                serializableList.Add(new Tuple<long, long, HashSet<ITransactionalGrain>>(r.LSN, r.TransactionId, r.Grains));
            }

            var sw = new BinaryTokenStreamWriter();
            SerializationManager.Serialize(serializableList, sw);

            return System.Convert.ToBase64String(sw.ToByteArray());
        }

        private List<CommitRecord> DeserializeCommitRecords(string base64)
        {
            var bytes = System.Convert.FromBase64String(base64);
            var sr = new BinaryTokenStreamReader(bytes);
            var l = SerializationManager.Deserialize<List<Tuple<long, long, HashSet<ITransactionalGrain>>>>(sr);
            var list = new List<CommitRecord>();
            foreach (var r in l)
            {
                list.Add(new CommitRecord() { LSN = r.Item1, TransactionId = r.Item2, Grains = r.Item3 });
            }
            return list;
        }

    }
}
