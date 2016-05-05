
using System.Net;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;

namespace Orleans.Messaging.Protocol
{
    internal interface IInterSiloProtocol
    {
        Task<IChannel> ConnectToSilo(IPEndPoint serverEndPoint);
    }
}
