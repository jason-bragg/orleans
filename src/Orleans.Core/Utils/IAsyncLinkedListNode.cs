
using System.Threading.Tasks;

namespace Orleans
{
    internal interface IAsyncLinkedListNode<T>
    {
        T Value { get; }
        Task<IAsyncLinkedListNode<T>> NextAsync { get; }
    }
}
