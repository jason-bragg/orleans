using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using System.Diagnostics;

namespace Orleans.Transactions
{
    /// <summary>
    /// Stateful facet that respects Orleans transaction semantics
    /// </summary>
    public class NoOpTransactionalState<TState> : ITransactionalState<TState>, ILifecycleParticipant<IGrainLifecycle>
        where TState : class, new()
    {
        private readonly ITransactionalStateConfiguration config;
        private readonly IGrainActivationContext context;
        private readonly ITransactionDataCopier<TState> copier;
        private readonly Dictionary<Type, object> copiers;
        private readonly IProviderRuntime runtime;
        private readonly IGrainRuntime grainRuntime;
        private readonly ILoggerFactory loggerFactory;
        private readonly JsonSerializerSettings serializerSettings;

        private ILogger logger;
        private ParticipantId participantId;

        private bool detectReentrancy;

        private TState state = new TState();
        private CallTime callTime;

        public NoOpTransactionalState(
            ITransactionalStateConfiguration transactionalStateConfiguration,
            IGrainActivationContext context,
            ITransactionDataCopier<TState> copier,
            IProviderRuntime runtime,
            IGrainRuntime grainRuntime,
            ILoggerFactory loggerFactory,
            JsonSerializerSettings serializerSettings
            )
        {
            this.config = transactionalStateConfiguration;
            this.context = context;
            this.copier = copier;
            this.runtime = runtime;
            this.grainRuntime = grainRuntime;
            this.loggerFactory = loggerFactory;
            this.serializerSettings = serializerSettings;
            this.copiers = new Dictionary<Type, object>();
            this.copiers.Add(typeof(TState), copier);
        }

        /// <summary>
        /// Read the current state.
        /// </summary>
        public Task<TResult> PerformRead<TResult>(Func<TState, TResult> operation)
        {
            if (detectReentrancy)
            {
                throw new LockRecursionException("cannot perform a read operation from within another operation");
            }

            this.callTime.Watch.Restart();

            var info = (TransactionInfo)TransactionContext.GetRequiredTransactionInfo<TransactionInfo>();

            info.Participants.TryGetValue(this.participantId, out var recordedaccesses);

            // record this read in the transaction info data structure
            info.RecordRead(this.participantId, info.TimeStamp);

            // perform the read 
            TResult result = default(TResult);
            try
            {
                detectReentrancy = true;

                result = CopyResult(operation(this.state));
            }
            finally
            {
                detectReentrancy = false;
            }

            return Task.FromResult(result);
        }

        /// <inheritdoc/>
        public Task<TResult> PerformUpdate<TResult>(Func<TState, TResult> updateAction)
        {
            if (updateAction == null) throw new ArgumentNullException(nameof(updateAction));
            if (detectReentrancy)
            {
                throw new LockRecursionException("cannot perform an update operation from within another operation");
            }

            this.callTime.Watch.Restart();

            var info = (TransactionInfo)TransactionContext.GetRequiredTransactionInfo<TransactionInfo>();

            if (info.IsReadOnly)
            {
                throw new OrleansReadOnlyViolatedException(info.Id);
            }

            info.Participants.TryGetValue(this.participantId, out var recordedaccesses);


            // record this write in the transaction info data structure
            info.RecordWrite(this.participantId, info.TimeStamp);

            // perform the write
            try
            {
                detectReentrancy = true;

                return Task.FromResult(CopyResult(updateAction(this.state)));
            }
            finally
            {
                detectReentrancy = false;
            }
        }

        public void Participate(IGrainLifecycle lifecycle)
        {
            lifecycle.Subscribe<TransactionalState<TState>>(GrainLifecycleStage.SetupState, (ct) => OnSetupState(ct, SetupResourceFactory));
        }

        private void SetupResourceFactory(IGrainActivationContext context, string stateName)
        {
            // Add resources factory to the grain context
            context.RegisterResourceFactory<ITransactionalResource>(stateName, () => new NoOpTransactionalResource(this.callTime));

            // Add tm factory to the grain context
            context.RegisterResourceFactory<ITransactionManager>(stateName, () => new NoOpTransactionManager(this.callTime));
        }

        internal Task OnSetupState(CancellationToken ct, Action<IGrainActivationContext, string> setupResourceFactory)
        {
            if (ct.IsCancellationRequested) return Task.CompletedTask;
            this.participantId = new ParticipantId(this.config.StateName, this.context.GrainInstance.GrainReference, ParticipantId.Role.Resource | ParticipantId.Role.Manager);
            this.logger = this.loggerFactory.CreateLogger($"{context.GrainType.Name}.{this.config.StateName}.{this.context.GrainIdentity.IdentityString}");
            this.callTime = new CallTime(this.logger);

            setupResourceFactory(this.context, this.config.StateName);

            return Task.CompletedTask;
        }

        private TResult CopyResult<TResult>(TResult result)
        {
            ITransactionDataCopier<TResult> resultCopier;
            if (!this.copiers.TryGetValue(typeof(TResult), out object cp))
            {
                resultCopier = this.context.ActivationServices.GetRequiredService<ITransactionDataCopier<TResult>>();
                this.copiers.Add(typeof(TResult), resultCopier);
            }
            else
            {
                resultCopier = (ITransactionDataCopier<TResult>)cp;
            }
            return resultCopier.DeepCopy(result);
        }

        private class CallTime
        {
            private static Timer _timer;
            private static long _maxMs;
            private static long _accumulatedMs;
            private static long _count;
            private static ILogger _logger;

            private readonly ILogger logger;
            private long maxMs;
            private long accumulatedMs;
            private long count = -1;
            private long lastReportedCount;

            public Stopwatch Watch { get; set; }

            static CallTime()
            {
                _timer = new Timer(LogReport, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(60));
            }

            public CallTime(ILogger logger)
            {
                this.Watch = new Stopwatch();
                this.logger = logger;
                _logger = _logger ?? logger;
            }

            public void Report()
            {
                if(this.count == -1)
                {
                    this.count = 0;
                    return; // ignore activation
                }
                this.Watch.Stop();
                long deltaMs = this.Watch.ElapsedMilliseconds;
                this.maxMs = Math.Max(this.maxMs, deltaMs);
                this.accumulatedMs += deltaMs;
                this.count++;
                if (this.count++ % 10 == 1)
                {
                    Interlocked.Add(ref _accumulatedMs, this.accumulatedMs);
                    this.accumulatedMs = 0;
                    Interlocked.Add(ref _count, this.count - this.lastReportedCount);
                    this.lastReportedCount = this.count;
                    Interlocked.Exchange(ref _maxMs, Math.Max(_maxMs, this.maxMs));
                }
            }

            private static void LogReport(object o)
            {
                if(_count != 0)
                    _logger.LogInformation("Prepare delta. averageMs: {AverageMs}, count: {Count}, maxMs: {MaxMs}", (long)Math.Floor((double)_accumulatedMs / _count), _count, _maxMs);
            }
        }

        private class NoOpTransactionManager : ITransactionManager
        {
            private readonly CallTime calltime;

            public NoOpTransactionManager(CallTime calltime)
            {
                this.calltime = calltime;
            }

            public Task<TransactionalStatus> PrepareAndCommit(Guid transactionId, AccessCounter accessCount, DateTime timeStamp, List<ParticipantId> writeResources, int totalResources)
            {
                this.calltime.Report();
                return Task.FromResult(TransactionalStatus.Ok);
            }

            public Task Prepared(Guid transactionId, DateTime timeStamp, ParticipantId resource, TransactionalStatus status)
            {
                return Task.CompletedTask;
            }

            public Task Ping(Guid transactionId, DateTime timeStamp, ParticipantId resource)
            {
                return Task.CompletedTask;
            }
        }

        private class NoOpTransactionalResource : ITransactionalResource
        {
            private readonly CallTime calltime;

            public NoOpTransactionalResource(CallTime calltime)
            {
                this.calltime = calltime;
            }

            public Task<TransactionalStatus> CommitReadOnly(Guid transactionId, AccessCounter accessCount, DateTime timeStamp)
            {
                return Task.FromResult(TransactionalStatus.Ok);
            }

            public Task Abort(Guid transactionId)
            {
                return Task.CompletedTask;
            }

            public Task Cancel(Guid transactionId, DateTime timeStamp, TransactionalStatus status)
            {
                return Task.CompletedTask;
            }

            public Task Confirm(Guid transactionId, DateTime timeStamp)
            {
                return Task.CompletedTask;
            }

            public Task Prepare(Guid transactionId, AccessCounter accessCount, DateTime timeStamp, ParticipantId transactionManager)
            {
                this.calltime.Report();
                return Task.CompletedTask;
            }
        }

    }
}
