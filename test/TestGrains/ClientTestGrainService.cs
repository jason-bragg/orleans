using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.Services;

namespace UnitTests.Grains
{
    public interface IClientTestCallback : IGrainExtension
    {
        Task ReturnCall(Guid id);
    }

    public class ClientTestCallback : IClientTestCallback
    {
        public ConcurrentBag<Guid> Called { get; } = new ConcurrentBag<Guid>();
        public Task ReturnCall(Guid id)
        {
            this.Called.Add(id);
            return Task.CompletedTask;
        }
    }

    public interface IClientTestGrainService : IGrainService
    {
        Task CallMe(IClientTestCallback clientCallback, Guid id);
    }

    public interface IClientTestGrainServiceLookupGrain : IGrainWithIntegerKey
    {
        Task Register(IClientTestGrainService service);
        Task<List<IClientTestGrainService>> Lookup();
    }

    public class ClientTestGrainServiceLookupGrain : Grain, IClientTestGrainServiceLookupGrain
    {
        List<IClientTestGrainService> services = new List<IClientTestGrainService>();

        public Task<List<IClientTestGrainService>> Lookup()
        {
            return Task.FromResult(this.services);
        }

        public Task Register(IClientTestGrainService service)
        {
            this.services.Add(service);
            return Task.CompletedTask;
        }
    }
}
