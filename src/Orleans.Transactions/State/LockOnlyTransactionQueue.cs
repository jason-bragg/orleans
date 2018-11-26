using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;
using Orleans.Configuration;

namespace Orleans.Transactions.State
{
    internal class LockOnlyTransactionQueue<TState> : ITransactionQueue<TState>
        where TState : class, new()
    {
        private readonly TransactionalStateOptions options;
        private readonly ParticipantId resource;
        protected readonly ILogger logger;

        private TState stableState;
        private long stableSequenceNumber;
        public ReadWriteLock<TState> RWLock { get; }
        public CausalClock Clock { get; }

        public LockOnlyTransactionQueue(
            IOptions<TransactionalStateOptions> options,
            ParticipantId resource,
            IClock clock,
            ILogger logger)
        {
            this.options = options.Value;
            this.resource = resource;
            this.Clock = new CausalClock(clock);
            this.logger = logger;
            this.RWLock = new ReadWriteLock<TState>(options, this, null, logger);
            this.stableState = new TState();
            this.stableSequenceNumber = 0;
        }

        public  Task EnqueueCommit(TransactionRecord<TState> record)
        {
            this.stableState = record.State;
            this.stableSequenceNumber = record.SequenceNumber;
            record.PromiseForTA?.TrySetResult(TransactionalStatus.Ok);
            return Task.CompletedTask;
        }

        public Task NotifyOfPrepared(Guid transactionId, DateTime timeStamp, TransactionalStatus status)
        {
            return Task.CompletedTask;
        }

        public async Task NotifyOfPrepare(Guid transactionId, AccessCounter accessCount, DateTime timeStamp, ParticipantId transactionManager)
        {
            var locked = await this.RWLock.ValidateLock(transactionId, accessCount);
            var status = locked.Item1;
            var record = locked.Item2;
            var valid = status == TransactionalStatus.Ok;

            record.Timestamp = timeStamp;
            record.Role = CommitRole.RemoteCommit; // we are not the TM
            record.TransactionManager = transactionManager;
            record.LastSent = null;
            record.PrepareIsPersisted = false;

            if (!valid)
            {
                await this.NotifyOfAbort(record, status);
            }
            else
            {
                this.Clock.Merge(record.Timestamp);
            }

            this.RWLock.Notify();
        }

        public async Task NotifyOfAbort(TransactionRecord<TState> entry, TransactionalStatus status)
        {
            switch (entry.Role)
            {
                case CommitRole.NotYetDetermined:
                    {
                        // cannot notify anyone. TA will detect broken lock during prepare.
                        break;
                    }
                case CommitRole.RemoteCommit:
                    {
                        if (logger.IsEnabled(LogLevel.Trace))
                            logger.Trace("aborting status={Status} {Entry}", status, entry);

                        entry.ConfirmationResponsePromise?.TrySetException(new OrleansException($"Confirm failed: Status {status}"));

                        if (entry.LastSent.HasValue)
                            return; // cannot abort anymore if we already sent prepare-ok message

                        if (logger.IsEnabled(LogLevel.Trace))
                            logger.Trace("aborting via Prepared. Status={Status} Entry={Entry}", status, entry);

                        entry.TransactionManager.Reference.AsReference<ITransactionManagerExtension>()
                             .Prepared(entry.TransactionManager.Name, entry.TransactionId, entry.Timestamp, resource, status)
                             .Ignore();
                        break;
                    }
                case CommitRole.LocalCommit:
                    {
                        if (logger.IsEnabled(LogLevel.Trace))
                            logger.Trace("aborting status={Status} {Entry}", status, entry);

                        try
                        {
                            // tell remote participants
                            await Task.WhenAll(entry.WriteParticipants
                                .Where(p => !p.Equals(resource))
                                .Select(p => p.Reference.AsReference<ITransactionalResourceExtension>()
                                     .Cancel(p.Name, entry.TransactionId, entry.Timestamp, status)));
                        }
                        catch (Exception ex)
                        {
                            this.logger.LogWarning(ex, "Failed to notify all transaction participants of cancellation.  TransactionId: {TransactionId}, Timestamp: {Timestamp}, Status: {Status}", entry.TransactionId, entry.Timestamp, status);
                        }

                        // reply to transaction agent
                        entry.PromiseForTA.TrySetResult(status);

                        break;
                    }
                case CommitRole.ReadOnly:
                    {
                        if (logger.IsEnabled(LogLevel.Trace))
                            logger.Trace("aborting status={Status} {Entry}", status, entry);

                        // reply to transaction agent
                        entry.PromiseForTA.TrySetResult(status);

                        break;
                    }
                default:
                    {
                        logger.LogError(777, "internal error: impossible case {CommitRole}", entry.Role);
                        throw new NotSupportedException($"{entry.Role} is not a supported CommitRole.");
                    }
            }
        }

        public Task NotifyOfPing(Guid transactionId, DateTime timeStamp, ParticipantId resource)
        {
            return Task.CompletedTask;
        }

        public Task NotifyOfConfirm(Guid transactionId, DateTime timeStamp)
        {
            return Task.CompletedTask;
        }

        public Task NotifyOfCancel(Guid transactionId, DateTime timeStamp, TransactionalStatus status)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// called on activation, and when recovering from storage conflicts or other exceptions.
        /// </summary>
        public Task NotifyOfRestore()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Ensures queue is ready to process requests.
        /// </summary>
        /// <returns></returns>
        public Task Ready()
        {
            return Task.CompletedTask;
        }

        public void GetMostRecentState(out TState state, out long sequenceNumber)
        {
            state = this.stableState;
            sequenceNumber = this.stableSequenceNumber;
        }

        protected virtual void OnLocalCommit(TransactionRecord<TState> entry)
        {
        }
    }
}
