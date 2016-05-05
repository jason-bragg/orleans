
using System;
using System.Net;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;
using Orleans.Messaging.Protocol;
using Orleans.Runtime.Configuration;

namespace Orleans.Runtime
{
    internal class ChannelManager
    {
        private readonly LRU<IPEndPoint, Task<IChannel>> cache;

        private const int MAX_CHANNELS = 200;

        internal ChannelManager(IMessagingConfiguration config)
        {
            IInterSiloProtocol protocol = InterSiloProtocol.V1;
            cache = new LRU<IPEndPoint, Task<IChannel>>(MAX_CHANNELS, config.MaxSocketAge,  protocol.ConnectToSilo);
            cache.RaiseFlushEvent += FlushHandler;
        }

        public Task<IChannel> GetChannel(IPEndPoint serverEndPoint)
        {
            return cache.Get(serverEndPoint);
        }

        private static void FlushHandler(Object sender, LRU<IPEndPoint, Task<IChannel>>.FlushEventArgs args)
        {
            if (args.Value == null) return;

            CloseChannel(args.Value).Ignore();
            NetworkingStatisticsGroup.OnClosedSendingSocket();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal void InvalidateEntry(IPEndPoint target)
        {
            Task<IChannel> channelTask;
            if (!cache.RemoveKey(target, out channelTask)) return;

            CloseChannel(channelTask).Ignore();
            NetworkingStatisticsGroup.OnClosedSendingSocket();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        // Note that this method assumes that there are no other threads accessing this object while this method runs.
        // Since this is true for the MessageCenter's use of this object, we don't lock around all calls to avoid the overhead.
        internal void Stop()
        {
            // Clear() on an LRU<> calls the flush handler on every item, so no need to manually close the sockets.
            cache.Clear();
        }

        private static async Task CloseChannel(Task<IChannel> channelTask)
        {
            IChannel channel = await channelTask;
            if (channel.Open)
            {
                await channel.CloseAsync();
            }
        }
    }
}
