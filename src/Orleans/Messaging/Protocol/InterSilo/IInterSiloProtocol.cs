
using System.Net;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;
using Orleans.Runtime;

namespace Orleans.Messaging.Protocol
{
    internal interface IInterSiloProtocol
    {
        Task<IChannel> ConnectToSilo(IPEndPoint serverEndPoint);
        Task<IChannel> Listen(IPEndPoint serverEndPoint, IMessageHandler messageHandler);
    }
}
