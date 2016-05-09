
using Orleans.Runtime;

namespace Orleans.Messaging.Protocol
{
    internal interface IMessageHandler
    {
        void HandleMessage(Message msg);
    }
}
