
using System;
using System.Net;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;
using Orleans.Messaging.Protocol;

namespace Orleans.Runtime.Messaging
{
    internal class IncomingMessageAcceptor : IMessageHandler
    {
        private bool listening;
        private readonly TraceLogger logger;
        private readonly MessageCenter messageCenter;
        private readonly IPEndPoint listenAddress;

        private Action<Message> sniffIncomingMessageHandler;
        private IChannel listener;

        // Used for holding enough info to handle receive completion
        internal IncomingMessageAcceptor(MessageCenter msgCtr, IPEndPoint here)
        {
            messageCenter = msgCtr;
            listenAddress = here ?? messageCenter.MyAddress.Endpoint;
            logger = TraceLogger.GetLogger("IncomingMessageAcceptor-" + listenAddress, TraceLogger.LoggerType.Runtime);
            logger.Info(ErrorCode.Messaging_IMA_OpenedListeningSocket, "Opened a listening socket at address " + listenAddress);
            listening = false;
        }

        public Action<Message> SniffIncomingMessage
        {
            set
            {
                if (sniffIncomingMessageHandler != null)
                    throw new InvalidOperationException("IncomingMessageAcceptor SniffIncomingMessage already set");

                sniffIncomingMessageHandler = value;
            }
        }

        public void Start()
        {
            StartAsync().Ignore();
        }

        private async Task StartAsync()
        {
            listening = true;
            while (listener == null && listening)
            {
                try
                {
                    listener = await InterSiloProtocol.V1.Listen(listenAddress, this);
                }
                catch (Exception ex)
                {
                    logger.Error(ErrorCode.MessagingBeginAcceptSocketException, "Exception beginning accept on listening socket", ex);
                }
                if (listener == null)
                {
                    await Task.Delay(100);
                    continue;
                }
                if (logger.IsVerbose3) logger.Verbose3("Started accepting connections.");
            }
        }

        public void Stop()
        {
            StopAsync().Ignore();
        }

        private async Task StopAsync()
        {
            listening = false;
            IChannel localListener = listener;
            listener = null;
            if (!Equals(localListener, null))
            {
                if (localListener.Open)
                {
                    if (logger.IsVerbose) logger.Verbose("Disconnecting the listening channel");
                    try
                    {
                        await localListener.CloseAsync();
                    }
                    catch (Exception ex)
                    {
                        logger.Error(ErrorCode.Messaging_IMA_FailedToShutdownListener, "Exception closing on listening channel", ex);
                    }
                }
            }
            if (logger.IsVerbose) logger.Verbose("Disconnecting the listening socket");
        }

        public void HandleMessage(Message msg)
        {
            // See it's a Ping message, and if so, short-circuit it
            object pingObj;
            var requestContext = msg.RequestContextData;
            if (requestContext != null &&
                requestContext.TryGetValue(RequestContext.PING_APPLICATION_HEADER, out pingObj) &&
                pingObj is bool &&
                (bool)pingObj)
            {
                MessagingStatisticsGroup.OnPingReceive(msg.SendingSilo);

                if (logger.IsVerbose2) logger.Verbose2("Responding to Ping from {0}", msg.SendingSilo);

                if (!msg.TargetSilo.Equals(messageCenter.MyAddress)) // got ping that is not destined to me. For example, got a ping to my older incarnation.
                {
                    MessagingStatisticsGroup.OnRejectedMessage(msg);
                    Message rejection = msg.CreateRejectionResponse(Message.RejectionTypes.Unrecoverable,
                        $"The target silo is no longer active: target was {msg.TargetSilo.ToLongString()}, but this silo is {messageCenter.MyAddress.ToLongString()}. The rejected ping message is {msg}.");
                    messageCenter.OutboundQueue.SendMessage(rejection);
                }
                else
                {
                    var response = msg.CreateResponseMessage();
                    response.BodyObject = Response.Done;
                    messageCenter.SendMessage(response);
                }
                return;
            }

            // sniff message headers for directory cache management
            sniffIncomingMessageHandler?.Invoke(msg);

            // Don't process messages that have already timed out
            if (msg.IsExpired)
            {
                msg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Receive);
                return;
            }

            // If we've stopped application message processing, then filter those out now
            // Note that if we identify or add other grains that are required for proper stopping, we will need to treat them as we do the membership table grain here.
            if (messageCenter.IsBlockingApplicationMessages && (msg.Category == Message.Categories.Application) && !Constants.SystemMembershipTableId.Equals(msg.SendingGrain))
            {
                // We reject new requests, and drop all other messages
                if (msg.Direction != Message.Directions.Request) return;

                MessagingStatisticsGroup.OnRejectedMessage(msg);
                var reject = msg.CreateRejectionResponse(Message.RejectionTypes.Unrecoverable, "Silo stopping");
                messageCenter.SendMessage(reject);
                return;
            }

            // Make sure the message is for us. Note that some control messages may have no target
            // information, so a null target silo is OK.
            if ((msg.TargetSilo == null) || msg.TargetSilo.Matches(messageCenter.MyAddress))
            {
                // See if it's a message for a client we're proxying.
                if (messageCenter.IsProxying && messageCenter.TryDeliverToProxy(msg)) return;

                // Nope, it's for us
                messageCenter.InboundQueue.PostMessage(msg);
                return;
            }

            if (!msg.TargetSilo.Endpoint.Equals(messageCenter.MyAddress.Endpoint))
            {
                // If the message is for some other silo altogether, then we need to forward it.
                if (logger.IsVerbose2) logger.Verbose2("Forwarding message {0} from {1} to silo {2}", msg.Id, msg.SendingSilo, msg.TargetSilo);
                messageCenter.OutboundQueue.SendMessage(msg);
                return;
            }

            // If the message was for this endpoint but an older epoch, then reject the message
            // (if it was a request), or drop it on the floor if it was a response or one-way.
            if (msg.Direction == Message.Directions.Request)
            {
                MessagingStatisticsGroup.OnRejectedMessage(msg);
                Message rejection = msg.CreateRejectionResponse(Message.RejectionTypes.Transient,
                    $"The target silo is no longer active: target was {msg.TargetSilo.ToLongString()}, but this silo is {messageCenter.MyAddress.ToLongString()}. The rejected message is {msg}.");
                messageCenter.OutboundQueue.SendMessage(rejection);
                if (logger.IsVerbose) logger.Verbose("Rejecting an obsolete request; target was {0}, but this silo is {1}. The rejected message is {2}.",
                    msg.TargetSilo.ToLongString(), messageCenter.MyAddress.ToLongString(), msg);
            }
        }
    }
}
