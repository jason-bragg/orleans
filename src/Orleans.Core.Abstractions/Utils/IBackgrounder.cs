using System;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Utils
{
    public interface IBackgrounder
    {
        Task<T> Run<T>(Func<CancellationToken, Task<T>> fetch);
    }

    public static class BackgrounderExtensions
    {
        public static Task Run(this IBackgrounder backgrounder, Func<CancellationToken, Task> fetch)
        {
            return backgrounder.Run(async (ct) =>
            {
                await fetch(ct);
                return true;
            });
        }
    }
}
