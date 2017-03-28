
using System.Threading.Tasks;
using Orleans.Storage;

namespace Orleans
{
    public interface IStorageBridge<TState>
        where TState : class, new()
    {
        TState State { get; set; }
        Task Initialize();
    }

    internal interface IPersistentGrain
    {
        void SetStorageProvider(IStorageProvider storageProvider);
    }

    internal interface IStatefulGrain<TState, TBridge> : IPersistentGrain
        where TBridge : IStorageBridge<TState>
        where TState : class, new()
    {
        TBridge Bridge { get; set; }
    }
}
