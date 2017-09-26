using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans.LeaseProviders;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Transactions
{
    public class TransactionIdGeneratorConfig
    {
        public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

        public string LeaseKey { get; set; } = "TransactionIdGenerator";

        public int AllocationBatchSize { get; set; } = 50000;

        public int LeaseRenewMaxRetry { get; set; } = 3;

        public void Copy(TransactionIdGeneratorConfig other)
        {
            if(other == null)
            {
                Copy(new TransactionIdGeneratorConfig());
            } else
            {
                this.LeaseDuration = other.LeaseDuration;
                this.LeaseKey = other.LeaseKey;
                this.AllocationBatchSize = other.AllocationBatchSize;
                this.LeaseRenewMaxRetry = other.LeaseRenewMaxRetry;
            }
        }
    }

    public class TransactionIdGenerator : ITransactionIdGenerator
    {
        // configuraiton
        private readonly TransactionIdGeneratorConfig config;

        // storage
        private Guid id;
        private readonly ITransactionIdGeneratorStorage storage;

        // leasing
        private readonly ILease lease;
        private readonly CancellationTokenSource active;
        private bool isLeaseOwner;
        private Task leaseAutoRenew;

        // transaction Ids
        private long[] bucket;
        private int index;
        private Task<long[]> nextBucket;
        
        private bool IsValid => !active.IsCancellationRequested && isLeaseOwner;

        public TransactionIdGenerator(IOptions<TransactionIdGeneratorConfig> config, ILeaseProvider leaseProvider, ITransactionIdGeneratorStorage storage)
        {
            this.active = new CancellationTokenSource();
            this.config = config.Value;

            LeaseRequest leaseRequest = new LeaseRequest(this.config.LeaseKey, this.config.LeaseDuration);
            this.lease = leaseProvider.CreateLease(nameof(TransactionIdGenerator), leaseRequest);

            this.storage = storage;
        }

        ~TransactionIdGenerator()
        {
            Dispose(false);
        }

        public static Factory<Task<ITransactionIdGenerator>> Create(IServiceProvider serviceProvider)
        {
            return async () =>
            {
                var generator = ActivatorUtilities.CreateInstance<TransactionIdGenerator>(serviceProvider, new object[0]);
                await generator.Initialize();
                return generator;
            };
        }

        private async Task Initialize()
        {
            this.id = Guid.NewGuid();

            // manage lease
            ResponseCode responseCode = await this.lease.Acquire();
            if(responseCode != ResponseCode.OK)
            {
                throw new TransactionIdGeneratorServiceNotAvailableException($"TransactionId generation failed to initialize.  Lease aquisition failed for lease {nameof(TransactionIdGenerator)}-{config.LeaseKey}.  ResponseCode: {responseCode}");
            }
            this.isLeaseOwner = true;
            this.leaseAutoRenew = LeaseAutoRenew();
            this.leaseAutoRenew.Ignore();

            // manage buckets
            this.nextBucket = this.storage.AllocateSequentialIds(this.id, config.AllocationBatchSize);
            this.nextBucket.Ignore();
            await GenerateMoreTransactionIds();
        }

        public async Task<long[]> GenerateTransactionIds(int count)
        {
            if (!IsValid)
                throw new TransactionIdGeneratorServiceNotAvailableException();
            long[] buffer = new long[count];
            int allocated = 0;
            while(allocated != count && this.IsValid)
            {
                allocated += await GenerateTransactionIds(buffer, allocated, count - allocated);
                if (!IsValid)
                    throw new TransactionIdGeneratorServiceNotAvailableException();
            }
            return buffer;
        }

        private async Task<int> GenerateTransactionIds(long[] buffer, int offset, int allocate)
        {
            int toCopy = Math.Min(allocate, bucket.Length - index);
            // TODO: We're using up the available id's even if their is a storage failure.  This will
            // waist id's when errors occur.  This shouldn't be an issue, but consider refactoring to avoid this.
            Array.Copy(bucket, index, buffer, offset, toCopy);
            index += toCopy;
            if (index == this.bucket.Length)
            {
                await GenerateMoreTransactionIds();
            }
            return toCopy;
        }

        private async Task GenerateMoreTransactionIds()
        {
            try
            {
                this.bucket = await this.nextBucket;
                this.index = 0;
            } catch(Exception)
            {
                // get next block anyway for next request
                this.nextBucket = this.storage.AllocateSequentialIds(this.id, config.AllocationBatchSize);
                this.nextBucket.Ignore();
                throw;
            }
            // we allways spin up a take for the next bucket so when it's needed we will not have to wait.
            this.nextBucket = this.storage.AllocateSequentialIds(this.id, config.AllocationBatchSize);
            this.nextBucket.Ignore();
        }

        private async Task LeaseAutoRenew()
        {
            int delay = (int)this.config.LeaseDuration.TotalMilliseconds / 2;
            int retryCount = 0;
            while (!active.IsCancellationRequested)
            {
                await Task.Delay(delay, this.active.Token);
                ResponseCode responseCode = await this.lease.Renew();
                switch (responseCode)
                {
                    case ResponseCode.OK:
                        {
                            delay = (int)this.config.LeaseDuration.TotalMilliseconds / 2;
                            retryCount = 0;
                            this.isLeaseOwner = true;
                            break;
                        }
                    case ResponseCode.InvalidToken:
                    case ResponseCode.LeaseNotAvailable:
                        {
                            this.isLeaseOwner = false;
                            // we've lost lease, we're done for.
                            this.active.Cancel();
                            return;
                        }
                    case ResponseCode.TransientFailure:
                        {
                            if(this.config.LeaseRenewMaxRetry < retryCount)
                            {
                                this.isLeaseOwner = false;
                                // we assume we've lost lease, we're done for.
                                this.active.Cancel();
                                return;
                            }
                            // Retry while not disposed, with shorter interval;
                            retryCount++;
                            delay = 0;
                            break;
                        }
                }
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (!disposing) return;
            this.active.Cancel();
        }

        #endregion
    }
}
