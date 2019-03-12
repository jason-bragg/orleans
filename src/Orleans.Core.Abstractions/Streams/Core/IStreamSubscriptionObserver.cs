using System.Threading.Tasks;

namespace Orleans.Streams
{
    public interface IStreamSubscriptionObserver
    {
        Task OnSubscribed(IStreamSubscriptionHandleFactory handleFactory);
    }
}
