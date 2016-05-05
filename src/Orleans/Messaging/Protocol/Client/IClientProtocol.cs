
using System.Net;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;

namespace Orleans.Messaging.Protocol.Client
{
    internal interface IClientProtocol
    {
        Task<IChannel> ConnectToServer(IPEndPoint serverEndPoint);
    }
}
