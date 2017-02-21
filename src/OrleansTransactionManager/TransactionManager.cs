using Orleans.Runtime;
using System.Collections.Generic;
using System.Threading;

namespace Orleans.Transactions
{
    public class TransactionManager : TransactionManagerBase
    {
        private readonly Thread dependencyThread;
        private readonly AutoResetEvent dependencyEvent;

        private readonly Thread commitThread;
        private readonly AutoResetEvent commitEvent;

        private readonly Thread checkpointThread;
        private readonly AutoResetEvent checkpointEvent;

        private long checkpointedLSN;

        private readonly Timer gcTimer;

        private readonly Logger logger;

        public TransactionManager(TransactionsConfiguration config)
            : base(config)
        {
            dependencyEvent = new AutoResetEvent(false);
            commitEvent = new AutoResetEvent(false);
            checkpointEvent = new AutoResetEvent(false);

            dependencyThread = new Thread(DependencyCompletionLoop);
            commitThread = new Thread(GroupCommitLoop);
            checkpointThread = new Thread(CheckpointLoop);
        }

        public override void Start()
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            this.StartAsync().Wait(cts.Token);
        }


        protected override void BeginDependencyCompletionLoop()
        {
            dependencyThread.Start();
        }

        protected override void BeginGroupCommitLoop()
        {
            commitThread.Start();
        }

        protected override void BeingCheckpointLoop()
        {
            checkpointThread.Start();
        }

        protected override void SignalDependencyEnqueued()
        {
            dependencyEvent.Set();
        }

        protected override void SignalGroupCommitEnqueued()
        {
            commitEvent.Set();
        }

        protected override void SignalCheckpointEnqueued()
        {
            checkpointEvent.Set();
        }

        private void DependencyCompletionLoop()
        {
            while (true)
            {
                dependencyEvent.WaitOne();
                base.CheckDependenciesCompleted();
            }
        }

        private void GroupCommitLoop()
        {
            while (true)
            {
                commitEvent.WaitOne();
                base.GroupCommit();
            }
        }

        private void CheckpointLoop()
        {
            Dictionary<ITransactionalGrain, long> grains = new Dictionary<ITransactionalGrain, long>();
            List<Transaction> transactions = new List<Transaction>();
            while (true)
            {
                // Maybe impose a max per batch to decrease latency?
                checkpointEvent.WaitOne();
                base.Checkpoint(grains, transactions);
            }
        }
    }
}
