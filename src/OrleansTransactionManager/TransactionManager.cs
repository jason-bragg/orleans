﻿using Orleans.Runtime;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    enum TransactionState
    {
        Started = 0,
        PendingDependency,
        Validated,
        Committed,
        Checkpointed,
        Aborted
    };

    public class TransactionManager : ITransactionManager
    {
        private TransactionsConfiguration config;

        private TransactionLog log;

        private ActiveTransactionsTracker activeTransactionsTracker;

        // Index of transactions by transactionId.
        private ConcurrentDictionary<long, Transaction> transactionsTable;

        private ConcurrentQueue<Transaction> notificationQueue;
        private Thread notificationThread;
        private AutoResetEvent notificationEvent;

        private ConcurrentQueue<Tuple<CommitRecord, Transaction>> groupCommitQueue;
        private Thread commitThread;
        private AutoResetEvent commitEvent;

        // Queue of committed transactions in commit order
        private ConcurrentQueue<Transaction> checkpointQueue;
        private Thread checkpointThread;
        private AutoResetEvent checkpointEvent;

        private long checkpointedLSN;

        private Timer gcTimer;

        private readonly Logger logger;

        public TransactionManager(TransactionsConfiguration config)
        {
            this.config = config;

            if (config.LogType == TransactionsConfiguration.TransactionLogType.AzureTable)
            {
                log = new AzureTransactionLog(config.DataConnectionString, "OrleansTransactions", config.ClearLogOnStartup);
            }
            else
            {
                log = new MemoryTransactionLog();
            }

            activeTransactionsTracker = new ActiveTransactionsTracker(config, log);

            transactionsTable = new ConcurrentDictionary<long, Transaction>(2, 1000000);

            notificationQueue = new ConcurrentQueue<Transaction>();
            groupCommitQueue = new ConcurrentQueue<Tuple<CommitRecord, Transaction>>();
            checkpointQueue = new ConcurrentQueue<Transaction>();

            notificationEvent = new AutoResetEvent(false);
            commitEvent = new AutoResetEvent(false);
            checkpointEvent = new AutoResetEvent(false);

            notificationThread = new Thread(DependenciesCompleted);
            commitThread = new Thread(GroupCommit);
            checkpointThread = new Thread(Checkpoint);

            checkpointedLSN = 0;

            gcTimer = new Timer(GC);
            
            logger = LogManager.GetLogger("TransactionManager");
            
        }

        #region ITransactionManager
        public void Start()
        {
            log.Initialize();
            CommitRecord record = log.GetFirstCommitRecord().Result;
            long prevLSN = 0;
            while (record != null)
            {
                Transaction tx = new Transaction(record.TransactionId) 
                { 
                    State = TransactionState.Committed,
                    LSN = record.LSN,
                    Info = new TransactionInfo(record.TransactionId)
                };

                if (prevLSN == 0)
                {
                    checkpointedLSN = record.LSN - 1;
                }
                else
                {
                    // There should be no gaps in the log
                    Debug.Assert(prevLSN == record.LSN - 1);
                }
                prevLSN = record.LSN;

                foreach (var grain in record.Grains)
                {
                    tx.Info.WriteSet.Add(grain, 1);
                }

                transactionsTable[record.TransactionId] = tx;
                checkpointQueue.Enqueue(tx);
                checkpointEvent.Set();

                record = log.GetNextCommitRecord().Result;
            }

            log.EndRecovery();
            var maxAllocatedTransactionId = log.GetStartRecord();
            activeTransactionsTracker.Start(maxAllocatedTransactionId);

            notificationThread.Start();
            commitThread.Start();
            checkpointThread.Start();

            gcTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(-1));
        }

        public long StartTransaction(TimeSpan timeout)
        {
            var transactionId = activeTransactionsTracker.GetNewTransactionId();

            Transaction tx = new Transaction(transactionId)
            {
                State = TransactionState.Started,
                ExpirationTime = DateTime.Now.Ticks + timeout.Ticks,
                Completion = new TaskCompletionSource<bool>()
            };

            transactionsTable[transactionId] = tx;

            return tx.TransactionId;
        }

        public void AbortTransaction(long transactionId, OrleansTransactionAbortedException reason)
        {
            Transaction tx;
            if (transactionsTable.TryGetValue(transactionId, out tx))
            {
                bool justAborted = false;
                lock (tx)
                {
                    if (tx.State == TransactionState.Started ||
                        tx.State == TransactionState.PendingDependency)
                    {
                        tx.State = TransactionState.Aborted;
                        justAborted = true;
                    }
                }

                if (justAborted)
                {
                    foreach (var waiting in tx.WaitingTransactions)
                    {
                        var cascading = new OrleansCascadingAbortException(waiting.Info.TransactionId, tx.TransactionId);
                        AbortTransaction(waiting.Info.TransactionId, cascading);
                    }

                    tx.Completion.SetException(reason);
                }
            }
        }

        public Task CommitTransaction(TransactionInfo transactionInfo)
        {
            Debug.Assert(transactionInfo != null);

            Transaction tx;
            if (transactionsTable.TryGetValue(transactionInfo.TransactionId, out tx))
            {
                bool abort = false;
                long cascadingDependentId = 0;

                bool pending = false;
                lock (tx)
                {
                    if (tx.State == TransactionState.Started)
                    {
                        tx.Info = transactionInfo;

                        // Check our dependent transactions.
                        // - If all dependent transactions committed, put in validating queue
                        // - If at least one dependent transaction aborted, abort
                        // - If at least one dependent transaction is still pending, put in 
                        //   pending queue
                        foreach (var dependentId in tx.Info.DependentTransactions)
                        {
                            Transaction dependentTx;
                            if (!transactionsTable.TryGetValue(dependentId, out dependentTx))
                            {
                                abort = true;
                                cascadingDependentId = dependentId;
                                break;
                            }

                            // NOTE: our deadlock prevention mechanism ensures that we are acquiring
                            // the locks in proper order and there is no risk of deadlock.
                            lock (dependentTx)
                            {
                                if (dependentTx.State == TransactionState.Aborted)
                                {
                                    abort = true;
                                    cascadingDependentId = dependentId;
                                    break;
                                }

                                if (dependentTx.State == TransactionState.Started ||
                                    dependentTx.State == TransactionState.PendingDependency)
                                {
                                    pending = true;
                                    dependentTx.WaitingTransactions.Add(tx);
                                    tx.PendingCount++;
                                }
                            }
                        }

                        if (abort)
                        {
                            AbortTransaction(transactionInfo.TransactionId, new OrleansCascadingAbortException(transactionInfo.TransactionId, cascadingDependentId));
                        }
                        else if (pending)
                        {
                            tx.State = TransactionState.PendingDependency;
                        }
                        else
                        {
                            tx.State = TransactionState.Validated;
                            notificationQueue.Enqueue(tx);
                            notificationEvent.Set();
                        }

                    }
                }

                return tx.Completion.Task;

            }
            else
            {
                Debug.Assert(transactionInfo.TransactionId <= activeTransactionsTracker.GetHighestActiveTransactionId());

                // Don't have a record of the transaction any more so presumably it's aborted.
                throw new OrleansTransactionAbortedException(transactionInfo.TransactionId, "Transaction presumed to be aborted");
            }
        }

        public long GetReadOnlyTransactionId()
        {
            long readId = activeTransactionsTracker.GetSmallestActiveTransactionId();
            if (readId > 0)
            {
                readId--; 
            }
            return readId;
        }

        #endregion

        private void DependenciesCompleted()
        {
            while (true)
            {
                notificationEvent.WaitOne();

                Transaction tx = null;
                while (notificationQueue.TryDequeue(out tx))
                {
                    CommitRecord commitRecord = new CommitRecord();
                    foreach (var grain in tx.Info.WriteSet.Keys)
                    {
                        commitRecord.Grains.Add(grain);
                    }
                    groupCommitQueue.Enqueue(new Tuple<CommitRecord, Transaction>(commitRecord, tx));
                    commitEvent.Set();

                    // We don't need to hold the transaction lock any more to access
                    // the WaitingTransactions queue, since nothing can be added to it
                    // after this point.
                    foreach (var waiting in tx.WaitingTransactions)
                    {
                        lock (waiting)
                        {
                            if (waiting.State != TransactionState.Aborted)
                            {
                                Debug.Assert(waiting.State == TransactionState.PendingDependency);
                                Debug.Assert(waiting.PendingCount > 0);

                                waiting.PendingCount--;

                                if (waiting.PendingCount == 0)
                                {
                                    waiting.State = TransactionState.Validated;

                                    notificationQueue.Enqueue(waiting);
                                    notificationEvent.Set();
                                }
                            }
                        }
                    }
                }
            }
        }

        private void GroupCommit()
        {
            while (true)
            {
                commitEvent.WaitOne();

                int batchSize = groupCommitQueue.Count;
                List<CommitRecord> records = new List<CommitRecord>(batchSize);
                List<Transaction> transactions = new List<Transaction>(batchSize);
                while (batchSize > 0)
                {
                    Tuple<CommitRecord, Transaction> t;
                    groupCommitQueue.TryDequeue(out t);
                    records.Add(t.Item1);
                    transactions.Add(t.Item2);
                    batchSize--;
                }

                try
                {
                    logger.Verbose("Group Commit: Writing {0} transactions to log", transactions.Count);
                    log.Append(records).Wait();
                    logger.Verbose("Group Commit: Successfully written {0} transactions to log", transactions.Count);
                }
                catch (Exception e)
                {
                    logger.Error(0, "Group Commit error", e);
                    // Failure to get an acknowledgment of the commits from the log (e.g. timeout exception)
                    // will put the transactions in doubt. We crash and let this be handled in recovery.
                    // TODO: handle other exceptions more gracefuly
                    throw e;

                }

                for (int i = 0; i < transactions.Count; i++)
                {
                    var transaction = transactions[i];
                    lock (transaction)
                    {
                        transaction.State = TransactionState.Committed;
                        transaction.LSN = records[i].LSN;
                    }
                    transaction.Completion.SetResult(true);
                    checkpointQueue.Enqueue(transaction);
                    checkpointEvent.Set();
                }
            }
        }

        private void Checkpoint()
        {
            Dictionary<ITransactionalGrain, long> grains = new Dictionary<ITransactionalGrain,long>();
            List<Transaction> transactions = new List<Transaction>();
            while (true)
            {
                // Maybe impose a max per batch to decrease latency?
                checkpointEvent.WaitOne();

                int batchSize = checkpointQueue.Count;
                long lsn = 0;
                grains.Clear();
                transactions.Clear();

                while (batchSize > 0)
                {
                    Transaction tx;
                    checkpointQueue.TryDequeue(out tx);
                    foreach (var grain in tx.Info.WriteSet.Keys)
                    {
                        grains[grain] = tx.Info.TransactionId;
                    }
                    lsn = tx.LSN;
                    transactions.Add(tx);
                    batchSize--;
                }

                Task[] tasks = new Task[grains.Count];
                int i = 0;
                foreach (var g in grains)
                {
                    tasks[i++] = g.Key.Commit(g.Value);
                }

                try
                {
                    logger.Verbose("Checkpoint: Calling Commit on grains for {0} transactions", transactions.Count);
                    // Note: These waits can be moved to a separate step in the pipeline if need be.
                    Task.WaitAll(tasks);
                    logger.Verbose("Checkpoint: Successfully called Commit on grains for {0} transactions", transactions.Count);
                }
                catch (Exception e)
                {
                    logger.Error(0, "Failure during checkpoint", e);
                    throw e;
                }

                foreach (var tx in transactions)
                {
                    lock (tx)
                    {
                        tx.State = TransactionState.Checkpointed;
                        tx.HighestActiveTransactionIdAtCheckpoint = activeTransactionsTracker.GetHighestActiveTransactionId();
                    }
                }

                if (transactions.Count > 0)
                {
                    Debug.Assert(lsn > this.checkpointedLSN);
                    this.checkpointedLSN = lsn;
                }
            }
        }

        private void GC(object args)
        {
            //
            // Truncate log
            //
            if (checkpointedLSN > 0)
            {
                try
                {
                    log.TruncateLog(checkpointedLSN - 1).Wait();
                }
                catch (Exception e)
                {
                    logger.Error(0, string.Format("Failed to truncate log. LSN: {0}", checkpointedLSN), e);
                }
            }

            //
            // Timeout expired transactions
            //
            long now = DateTime.Now.Ticks;
            foreach (var txRecord in transactionsTable)
            {
                if (txRecord.Value.State == TransactionState.Started &&
                    txRecord.Value.ExpirationTime < now)
                {
                    AbortTransaction(txRecord.Key, new OrleansTransactionTimeoutException(txRecord.Key));
                }
            }

            //
            // Find the oldest active transaction
            //
            long lowestActiveId = activeTransactionsTracker.GetSmallestActiveTransactionId();
            long highestActiveId = activeTransactionsTracker.GetHighestActiveTransactionId();
            while (lowestActiveId <= highestActiveId)
            {
                Transaction tx = null;

                if (transactionsTable.TryGetValue(lowestActiveId, out tx))
                {
                    if (tx.State != TransactionState.Aborted &&
                        tx.State != TransactionState.Checkpointed)
                    {
                        break;
                    }
                }

                lowestActiveId++;
                activeTransactionsTracker.PopSmallestActiveTransactionId();
            }

            //
            // Remove transactions that we no longer need to keep a record of from transactions table.
            // a transaction is presumed to be aborted if we try to look it up and it does not exist in the
            // table.
            //
            foreach (var txRecord in transactionsTable)
            {
                if (txRecord.Value.State == TransactionState.Aborted)
                {
                    Transaction temp;
                    transactionsTable.TryRemove(txRecord.Key, out temp);
                }
                else if (txRecord.Value.State == TransactionState.Checkpointed)
                {
                    lock (txRecord.Value)
                    {
                        if (txRecord.Value.HighestActiveTransactionIdAtCheckpoint < activeTransactionsTracker.GetSmallestActiveTransactionId())
                        {
                            // The oldest active transaction started after this transaction was checkpointed
                            // so no in progress transaction is going to take a dependency on this transaction
                            // which means we can safely forget about it.
                            Transaction temp;
                            transactionsTable.TryRemove(txRecord.Key, out temp);
                        }
                    }
                } 
            }

            //
            // Schedule next GC cycle
            //
            gcTimer.Change(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(-1));

        }

    }
}
