using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;
using Orleans.Runtime;

namespace Orleans.Messaging
{
    /// <summary>
    /// The GatewayConnection class does double duty as both the manager of the connection itself (the socket) and the sender of messages
    /// to the gateway. It uses a single instance of the Receiver class to handle messages from the gateway.
    /// 
    /// Note that both sends and receives are synchronous.
    /// </summary>
    internal class GatewayConnection : AsynchQueueAgent<Message>
    {
        internal bool IsLive { get; private set; }
        internal ProxiedMessageCenter MsgCenter { get; private set; }

        private Uri addr;

        internal Uri Address
        {
            get { return addr; }
            private set
            {
                addr = value;
                Silo = addr.ToSiloAddress();
            }
        }

        internal SiloAddress Silo { get; private set; }

        internal IChannel Channel { get; private set; }

        private DateTime lastConnect;

        internal GatewayConnection(Uri address, ProxiedMessageCenter mc)
            : base("GatewayClientSender_" + address, mc.MessagingConfiguration)
        {
            Address = address;
            MsgCenter = mc;
            lastConnect = new DateTime();
            IsLive = true;
        }

        public override void Start()
        {
        }


        protected override void Process(Message request)
        {
        }

        public override void Stop()
        {
        }

        protected override void RunNonBatching()
        {
            while (!Cts.IsCancellationRequested)
            {
                RunNonBatchingLoop().Wait();
            }
        }

        private async Task RunNonBatchingLoop()
        {
            const int MaxPendingSends = 10;

            if (!Channel.Open || Channel.Active)
            {
                await Connect();
                continue;
            }
            do
            {
                Message request;
                try
                {
                    request = requestQueue.Take();
                }
                catch (InvalidOperationException)
                {
                    Log.Info(ErrorCode.Runtime_Error_100312, "Stop request processed");
                    break;
                }
                Channel.WriteAsync(request).Wait();
            } while (!Cts.IsCancellationRequested && requestQueue.Count > 0);
            Channel.Flush();
        }

        private async Task Connect()
        {
            if (!MsgCenter.Running)
            {
                if (Log.IsVerbose) Log.Verbose(ErrorCode.ProxyClient_MsgCtrNotRunning, "Ignoring connection attempt to gateway {0} because the proxy message center is not running", Address);
                return;
            }

            // Yes, we take the lock around a Sleep. The point is to ensure that no more than one thread can try this at a time.
            // There's still a minor problem as written -- if the sending thread and receiving thread both get here, the first one
            // will try to reconnect. eventually do so, and then the other will try to reconnect even though it doesn't have to...
            // Hopefully the initial "if" statement will prevent that.
            List<Task> pending = new List<Task>();
            lock (Lockable)
            {
                if (!IsLive)
                {
                    if (Log.IsVerbose) Log.Verbose(ErrorCode.ProxyClient_DeadGateway, "Ignoring connection attempt to gateway {0} because this gateway connection is already marked as non live", Address);
                    return; // if the connection is already marked as dead, don't try to reconnect. It has been doomed.
                }

                for (var i = 0; i < ProxiedMessageCenter.CONNECT_RETRY_COUNT; i++)
                {
                    try
                    {
                        if (Channel != null)
                        {
                            if (Channel.Open && Channel.Active)
                                return;

                            pending.Add(MarkAsDisconnected(Channel)); // clean up the socket before reconnecting.
                        }
                        if (lastConnect != new DateTime())
                        {
                            var millisecondsSinceLastAttempt = DateTime.UtcNow - lastConnect;
                            if (millisecondsSinceLastAttempt < ProxiedMessageCenter.MINIMUM_INTERCONNECT_DELAY)
                            {
                                var wait = ProxiedMessageCenter.MINIMUM_INTERCONNECT_DELAY - millisecondsSinceLastAttempt;
                                if (Log.IsVerbose) Log.Verbose(ErrorCode.ProxyClient_PauseBeforeRetry, "Pausing for {0} before trying to connect to gateway {1} on trial {2}", wait, Address, i);
                                Thread.Sleep(wait);
                            }
                        }
                        lastConnect = DateTime.UtcNow;
                        Socket = new Socket(Silo.Endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                        Socket.Connect(Silo.Endpoint);
                        NetworkingStatisticsGroup.OnOpenedGatewayDuplexSocket();
                        SocketManager.WriteConnectionPreemble(Socket, MsgCenter.ClientId);  // Identifies this client
                        Log.Info(ErrorCode.ProxyClient_Connected, "Connected to gateway at address {0} on trial {1}.", Address, i);
                        return;
                    }
                    catch (Exception)
                    {
                        Log.Warn(ErrorCode.ProxyClient_CannotConnect, "Unable to connect to gateway at address {0} on trial {1}.", Address, i);
                        MarkAsDisconnected(Socket);
                    }
                }
                // Failed too many times -- give up
                MarkAsDead();
            }
        }

        private async Task<bool> TryConnect(int attempt)
        {
            try
            {
                if (Channel != null)
                {
                    if (Channel.Open && Channel.Active)
                        return true;

                    await MarkAsDisconnected(Channel);
                }
                if (lastConnect != new DateTime())
                {
                    var millisecondsSinceLastAttempt = DateTime.UtcNow - lastConnect;
                    if (millisecondsSinceLastAttempt < ProxiedMessageCenter.MINIMUM_INTERCONNECT_DELAY)
                    {
                        var wait = ProxiedMessageCenter.MINIMUM_INTERCONNECT_DELAY - millisecondsSinceLastAttempt;
                        if (Log.IsVerbose) Log.Verbose(ErrorCode.ProxyClient_PauseBeforeRetry, "Pausing for {0} before trying to connect to gateway {1} on trial {2}", wait, Address, i);
                        await Task.Delay(wait);
                    }
                }
                lastConnect = DateTime.UtcNow;
                connect
                NetworkingStatisticsGroup.OnOpenedGatewayDuplexSocket();
                Log.Info(ErrorCode.ProxyClient_Connected, "Connected to gateway at address {0} on trial {1}.", Address, attempt);
            }
            catch (Exception)
            {
                Log.Warn(ErrorCode.ProxyClient_CannotConnect, "Unable to connect to gateway at address {0} on trial {1}.", Address, attempt);
                return false;
            }
            return true;
        }

        private async Task MarkAsDisconnected(IChannel disconnectedChannel)
        {
            IChannel channel = disconnectedChannel;
            if (disconnectedChannel == null)
                return;
            lock (Lockable)
            {
                if (Equals(Channel, disconnectedChannel))  // handles races between connect and disconnect, since sometimes we grab the socket outside lock.
                {
                    Channel = null;
                    Log.Warn(ErrorCode.ProxyClient_MarkGatewayDisconnected,
                        $"Marking gateway at address {Address} as Disconnected");
                    MsgCenter?.GatewayManager?.ExpediteUpdateLiveGatewaysSnapshot();
                }
            }
            if (channel != null)
            {
                if(channel.Open) await channel.DisconnectAsync();
                NetworkingStatisticsGroup.OnClosedGatewayDuplexSocket();
            }
        }
    }
}
