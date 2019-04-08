using System;
using System.Threading.Tasks;
using Orleans.Core;

namespace Orleans.Streams
{
    internal class RecoverableStreamStorage<TState> : IRecoverableStreamStorage<TState>
    {
        private readonly IAdvancedStorage<RecoverableStreamState<TState>> storage;
        private readonly IRecoverableStreamStoragePolicy policy;

        private StreamSequenceToken lastKnownPersistedToken;

        private DateTime? nextCheckpointTime;
        private int checkpointAttemptCount;

        public RecoverableStreamStorage(IAdvancedStorage<RecoverableStreamState<TState>> storage, IRecoverableStreamStoragePolicy policy)
        {
            if (storage == null) { throw new ArgumentNullException(nameof(storage)); }
            if (policy == null) { throw new ArgumentNullException(nameof(policy)); }

            this.storage = storage;
            this.policy = policy;
        }

        public RecoverableStreamStorage(IStorage<RecoverableStreamState<TState>> storage, IRecoverableStreamStoragePolicy policy)
            : this(new AdvancedStorageSimpleStorageAdapter<RecoverableStreamState<TState>>(storage), policy)
        {
        }

        public RecoverableStreamState<TState> State { get; set; }

        public async Task Load()
        {
            int attempts = 0;

            // This loop will exit when the state is loaded
            // TODO: Should there be a max attempts here?
            while (true)
            {
                AdvancedStorageReadResultCode readResult;

                try
                {
                    ++attempts;
                    readResult = await this.storage.ReadStateAsync();
                }
                catch (Exception exception)
                {
                    readResult = AdvancedStorageReadResultCode.GeneralFailure;

                    // TODO: Log
                    Console.WriteLine(exception);
                }

                bool shouldBackoff;

                switch (readResult)
                {
                    case AdvancedStorageReadResultCode.Success:
                    {
                        if (this.nextCheckpointTime == null)
                        {
                            this.nextCheckpointTime = DateTime.UtcNow + this.policy.GetNextCheckpoint(0);
                        }

                        return;
                    }
                    case AdvancedStorageReadResultCode.Throttled:
                    {
                        shouldBackoff = true;
                        break;
                    }
                    case AdvancedStorageReadResultCode.GeneralFailure:
                    {
                        shouldBackoff = false;
                        break;
                    }
                    default:
                    {
                        throw new NotSupportedException(FormattableString.Invariant(
                            $"Unknown {nameof(AdvancedStorageReadResultCode)} '{readResult}' returned by storage"));
                    }
                }

                if (shouldBackoff)
                {
                    var backoff = this.policy.GetReadBackoff(readResult, attempts);

                    if (backoff > TimeSpan.Zero)
                    {
                        await Task.Delay(backoff);
                    }
                }
            }
        }

        // TODO: Not a fan of this code or Load even. It's hard to see the exit conditions. This will make logging difficult.
        // Return true if we FF'd. 
        public async Task<bool> Save()
        {
            int attempts = 0;

            // This loop will exit when either the state is saved or the state is synchronized with what's in storage
            // TODO: Should there be a max attempts here?
            while (true)
            {
                var currentStreamingState = this.storage.State;
                var currentStreamingStateETag = this.storage.ETag;

                AdvancedStorageWriteResultCode writeResult;

                try
                {
                    ++attempts;
                    writeResult = await this.storage.WriteStateAsync();
                }
                catch (Exception exception)
                {
                    writeResult = AdvancedStorageWriteResultCode.GeneralFailure;

                    // TODO: Log
                    Console.WriteLine(exception);
                }

                // TODO: For safety we should limit the number of "optimized / skipped reloads we do"
                bool shouldBackoff;
                bool shouldReload;

                switch (writeResult)
                {
                    case AdvancedStorageWriteResultCode.Success:
                        this.nextCheckpointTime = DateTime.Now + this.policy.GetNextCheckpoint(0);
                        return false; // We didn't need to FF
                    case AdvancedStorageWriteResultCode.Ambiguous:
                        shouldBackoff = this.policy.ShouldBackoffOnWriteWithAmbiguousResult;
                        shouldReload = this.policy.ShouldReloadOnWriteWithAmbiguousResult;
                        break;
                    case AdvancedStorageWriteResultCode.Conflict:
                        shouldBackoff = false;
                        shouldReload = true;
                        break;
                    case AdvancedStorageWriteResultCode.Throttled:
                        shouldBackoff = true;
                        shouldReload = false;
                        break;
                    case AdvancedStorageWriteResultCode.GeneralFailure:
                        // It's possible the storage implementation doesn't distinguish between failures. This means that even "Conflict" would look like a "General Failure". We must reload in this case - otherwise we'd never resolve the conflict. Storage implementations have an optimization opportunity to skip a potentially unnecessary load by signaling either "Ambiguous" or "Throttled".
                        shouldBackoff = true;
                        shouldReload = true;
                        break;
                    default:
                        // TODO: Log
                        throw new NotSupportedException(FormattableString.Invariant($"Unknown {nameof(AdvancedStorageWriteResultCode)} '{writeResult}' returned by storage"));
                }

                if (shouldBackoff)
                {
                    var backoff = this.policy.GetWriteBackoff(writeResult, attempts);

                    if (backoff > TimeSpan.Zero)
                    {
                        await Task.Delay(backoff);
                    }
                }

                if (shouldReload)
                {
                    // TODO: Maybe attempts should be carried in to here. Unfortunately right now "attempts" includes transient failures and conflicts and we probably only want to carry transient failures in.
                    await this.Load();

                    var storedStreamingState = this.storage.State;
                    var storedStreamingStateETag = this.storage.ETag;

                    if (currentStreamingStateETag != storedStreamingStateETag)
                    {
                        // The ETags don't match so something was actually written to storage. Let's comapre ot that.
                        var comparison = currentStreamingState.EasyCompareTo(storedStreamingState);

                        // TODO: Handle Idle differences

                        switch (comparison)
                        {
                            case EasyCompareToResult.Before:
                            {
                                // We're behind. Use what's currently in storage.

                                this.nextCheckpointTime = DateTime.Now + this.policy.GetNextCheckpoint(0);

                                return true; // We need to FF
                            }
                            case EasyCompareToResult.Equal:
                            {
                                // We're equal. We could take either state but we'll keep ours because maybe the GC implications are better.
                                this.storage.State = currentStreamingState;

                                this.nextCheckpointTime = DateTime.Now + this.policy.GetNextCheckpoint(0);

                                return false; // We didn't need to FF.
                            }
                            case EasyCompareToResult.After:
                            {
                                // We're ahead. Overwrite storage.
                                this.storage.State = currentStreamingState;

                                break;
                            }
                            default:
                            {
                                throw new NotSupportedException($"Unknown {nameof(EasyCompareToResult)} '{comparison}'");
                            }
                        }
                    }
                }

                // If we got this far it means we need to save
            }
        }

        public Task<(bool persisted, bool fastForwardRequested)> CheckpointIfOverdue()
        {
            if (this.nextCheckpointTime == null)
            {
                // TODO: Warn?
                return Task.FromResult((false, false));
            }

            if (DateTime.UtcNow < this.nextCheckpointTime)
            {
                return Task.FromResult((false, false));
            }

            // TODO: This can probably share code with Save. But we need to consider the checkpoint policy. If the checkpoint fails transiently, we should evaluate the retry policy and see how much we care (how many times do we care to retry?). Of course if it's conflict we probably want to resolve it. If it's general failure we probably need to reload because we won't know if the storage impl bothers to return special codes or if we're in a fallback.
            throw new NotImplementedException();
        }
    }
}