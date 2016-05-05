
using System.Net;
using System.Threading.Tasks;
using DotNetty.Buffers;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Orleans.Runtime;

namespace Orleans.Messaging.Protocol
{
    internal class InterSiloProtocol_v1 : IInterSiloProtocol
    {
        private readonly Bootstrap bootstrap;

        public InterSiloProtocol_v1()
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
                    pipeline.AddLast(new SiloHandshake());
                }));
        }

        public Task<IChannel> ConnectToSilo(IPEndPoint serverEndPoint)
        {
            return bootstrap.ConnectAsync(serverEndPoint);
        }

        private class SiloHandshake : ChannelHandlerAdapter
        {
            public override void ChannelActive(IChannelHandlerContext context)
            {
                byte[] handshakeBytes = Constants.SiloDirectConnectionId.ToByteArray();
                IByteBuffer handshakeBuffer = context.Allocator.Buffer(handshakeBytes.Length + sizeof(int)).WithOrder(ByteOrder.LittleEndian);
                handshakeBuffer.WriteInt(handshakeBytes.Length);
                handshakeBuffer.WriteBytes(handshakeBytes);
                context.WriteAndFlushAsync(handshakeBuffer).Ignore();
            }
        }
    }
}
