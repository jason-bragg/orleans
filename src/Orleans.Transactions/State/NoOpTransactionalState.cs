using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions.State;
using Orleans.Configuration;

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
        private NoOpTransactionalResource resource;

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

            this.resource.CallTimeStamp = DateTime.UtcNow;

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

            this.resource.CallTimeStamp = DateTime.UtcNow;

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
            context.RegisterResourceFactory<ITransactionalResource>(stateName, () => resource);

            // Add tm factory to the grain context
            context.RegisterResourceFactory<ITransactionManager>(stateName, () => new NoOpTransactionManager());
        }

        internal Task OnSetupState(CancellationToken ct, Action<IGrainActivationContext, string> setupResourceFactory)
        {
            if (ct.IsCancellationRequested) return Task.CompletedTask;
            this.participantId = new ParticipantId(this.config.StateName, this.context.GrainInstance.GrainReference, ParticipantId.Role.Resource | ParticipantId.Role.Manager);
            this.logger = this.loggerFactory.CreateLogger($"{context.GrainType.Name}.{this.config.StateName}.{this.context.GrainIdentity.IdentityString}");
            this.resource = new NoOpTransactionalResource(this.logger);

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

        private class NoOpTransactionManager : ITransactionManager
        {
            public Task<TransactionalStatus> PrepareAndCommit(Guid transactionId, AccessCounter accessCount, DateTime timeStamp, List<ParticipantId> writeResources, int totalResources)
            {
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
            private ILogger logger;

            public DateTime CallTimeStamp { get; set; }

            public NoOpTransactionalResource(ILogger logger)
            {
                this.logger = logger;
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
                this.logger.LogInformation("Prepare delta {Delta}ms", Math.Floor((DateTime.UtcNow - this.CallTimeStamp).TotalMilliseconds));
                return Task.CompletedTask;
            }
        }

    }
}
