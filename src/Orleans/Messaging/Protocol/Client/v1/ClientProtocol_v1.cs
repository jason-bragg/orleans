
using System.Net;
using System.Threading.Tasks;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Orleans.Messaging.Protocol.Client;

namespace Orleans.Messaging.Protocol
{
    internal class ClientProtocol_v1 : IClientProtocol
    {
        private readonly Bootstrap bootstrap;

        public ClientProtocol_v1()
        {
            bootstrap = new Bootstrap();
            var group = new MultithreadEventLoopGroup();
            bootstrap
                .Group(group)
                .Channel<TcpSocketChannel>()
                .Option(ChannelOption.TcpNodelay, true)
                .Handler(new ActionChannelInitializer<ISocketChannel>(channel =>
                {
                    IChannelPipeline pipeline = channel.Pipeline;
                    pipeline.AddLast(new MessageFrameEncoder());
                    pipeline.AddLast(new MessageFrameDecoder());
                    //pipeline.AddLast(new Blarg());
                }));
        }

        public Task<IChannel> ConnectToServer(IPEndPoint serverEndPoint)
        {
            return bootstrap.ConnectAsync(serverEndPoint);
        }
    }
}
