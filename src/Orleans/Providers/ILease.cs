using System;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.LeaseProviders
{

    public interface ILease
    {
        /// <summary>
        /// Acquire leases operation
        /// </summary>
        Task<ResponseCode> Acquire();

        /// <summary>
        /// Renew lease operation
        /// </summary>
        Task<ResponseCode> Renew();

        /// <summary>
        /// Release lease operation
        /// </summary>
        Task Release();
    }

    public static class LeaseExtensions
    {
        public static ILease CreateLease(this ILeaseProvider leaseProvider, string category, LeaseRequest leaseRequest)
        {
            return new Lease(leaseProvider, category, leaseRequest);
        }

        private class Lease : ILease
        {
            private readonly ILeaseProvider leaseProvider;
            private readonly string category;
            private readonly LeaseRequest[] leaseRequests;
            private AcquiredLease[] aquiredLeases;

            public Lease(ILeaseProvider leaseProvider, string category, LeaseRequest leaseRequest)
            {
                this.leaseProvider = leaseProvider;
                this.category = category;
                this.leaseRequests = new [] { leaseRequest };
            }

            public async Task<ResponseCode> Acquire()
            {
                AcquireLeaseResult[] results = await this.leaseProvider.Acquire(this.category, this.leaseRequests);
                AcquireLeaseResult result = results.FirstOrDefault();
                if (result == null) return ResponseCode.TransientFailure;
                if (result.StatusCode == ResponseCode.OK)
                {
                    this.aquiredLeases = new[] { result.AcquiredLease };
                }
                return result.StatusCode;
            }
            
            public async Task<ResponseCode> Renew()
            {
                AcquireLeaseResult[] results = await this.leaseProvider.Renew(this.category, this.aquiredLeases);
                AcquireLeaseResult result = results.FirstOrDefault();
                if (result == null) return ResponseCode.TransientFailure;
                if (result.StatusCode == ResponseCode.OK)
                {
                    this.aquiredLeases = new[] { result.AcquiredLease };
                }
                return result.StatusCode;
            }

            public async Task Release()
            {
                await this.leaseProvider.Release(this.category, this.aquiredLeases);
                this.aquiredLeases = null;
            }
        }
    }
}
