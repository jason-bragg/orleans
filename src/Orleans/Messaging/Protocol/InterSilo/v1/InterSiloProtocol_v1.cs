
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using Orleans.Runtime;

namespace Orleans.Messaging.Protocol
{
    internal class InterSiloProtocol_v1 : IInterSiloProtocol
    {
        private const int LISTEN_BACKLOG_SIZE = 1024;

        private readonly Lazy<Bootstrap> connector;
        private TraceLogger logger;

        public InterSiloProtocol_v1()
        {
            logger = TraceLogger.GetLogger("InterSiloProtocol_v1", TraceLogger.LoggerType.Runtime);

            connector = new Lazy<Bootstrap>(() =>
                new Bootstrap().Group(new MultithreadEventLoopGroup())
                    .Channel<TcpSocketChannel>()
                    .Option(ChannelOption.TcpNodelay, true)
                    .Handler(new ActionChannelInitializer<ISocketChannel>(channel =>
                    {
                        channel.Pipeline.AddLast(new MessageFrameEncoder());
                    })));
            }

        public async Task<IChannel> ConnectToSilo(IPEndPoint serverEndPoint)
        {
            logger.Warn(0, "InterSiloProtocol_v1.ConnectToSilo: " + serverEndPoint);
            IChannel channel = await connector.Value.ConnectAsync(serverEndPoint);
            logger.Warn(0, "InterSiloProtocol_v1.ConnectToSilo: " + serverEndPoint + " => " + channel.RemoteAddress);
            return channel;
        }

        public async Task<IChannel> Listen(IPEndPoint serverEndPoint, IMessageHandler messageHandler)
        {
            logger.Warn(0, "InterSiloProtocol_v1.Listen: " + serverEndPoint);

            if (messageHandler == null)
            {
                throw new ArgumentNullException("messageHandler");
            }

            ServerBootstrap listenerBuilder = new ServerBootstrap()
                .Group(new MultithreadEventLoopGroup(1), new MultithreadEventLoopGroup())
                .Channel<TcpServerSocketChannel>()
                .Option(ChannelOption.SoBacklog, LISTEN_BACKLOG_SIZE)
                .Option(ChannelOption.SoLinger, 0)
                .Option(ChannelOption.SoReuseaddr, true)
                .Handler(new ChannelLogger(logger))
                .ChildHandler(new ActionChannelInitializer<ISocketChannel>(channel =>
                {
                    channel.Pipeline
                        .AddLast(new MessageFrameDecoder())
                        .AddLast(new ChannelListner(messageHandler, logger));
                }));
            
            var listener = await listenerBuilder.BindAsync(serverEndPoint);
            logger.Warn(0, "InterSiloProtocol_v1.Listen: " + serverEndPoint + " => " + listener.LocalAddress);
            return listener;
        }

        private class ChannelLogger : ChannelHandlerAdapter
        {
            private readonly Logger logger;

            public ChannelLogger(Logger logger)
            {
                logger.Info(GetType().Name+".ChannelLogger");
                this.logger = logger;
            }

            public override void ChannelRegistered(IChannelHandlerContext context)
            {
                logger.Info(GetType().Name + ".ChannelRegistered: " + context.Channel.Id);
                base.ChannelRegistered(context);
            }

            public override void ChannelUnregistered(IChannelHandlerContext context)
            {
                logger.Info(GetType().Name + ".ChannelUnregistered: " + context.Channel.Id);
                base.ChannelUnregistered(context);
            }

            public override void ChannelActive(IChannelHandlerContext context)
            {
                logger.Info(GetType().Name + ".ChannelActive: " + context.Channel.Id);
                base.ChannelActive(context);
            }

            public override void ChannelInactive(IChannelHandlerContext context)
            {
                logger.Info(GetType().Name + ".ChannelInactive: " + context.Channel.Id);
                base.ChannelActive(context);
            }
        }

        private class ChannelListner : ChannelLogger
        {
            private readonly IMessageHandler handler;

            public ChannelListner(IMessageHandler messageHandler, Logger logger)
                : base(logger)
            {
                handler = messageHandler;
            }

            public override void ChannelRead(IChannelHandlerContext context, object message)
            {
                var msg = message as Message;
                if (msg != null)
                {
                    handler.HandleMessage(msg);
                }
            }
        }
    }
}
