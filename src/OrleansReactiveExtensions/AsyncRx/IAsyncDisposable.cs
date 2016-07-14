
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Rx
{
    public interface IAsyncDisposable
    {
        Task DisposeAsync(CancellationToken token = default(CancellationToken));
    }
}
