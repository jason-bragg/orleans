using System;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    public interface IAsyncLinkedListNode<TValue> : IDisposable
    {
        TValue Value { get; }
        Task<IAsyncLinkedListNode<TValue>> NextAsync { get; }
    }
}
