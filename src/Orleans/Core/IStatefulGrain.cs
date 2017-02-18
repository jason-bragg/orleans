
using System.Threading.Tasks;

namespace Orleans
{
    public interface IStorageBridge<TState>
        where TState : class, new()
    {
        TState State { get; set; }
        Task Initialize();
    }

    internal interface IStatefulGrain<TState, TBridge>
        where TBridge : IStorageBridge<TState>
        where TState : class, new()
    {
        TBridge Bridge { get; set; }
    }
}
