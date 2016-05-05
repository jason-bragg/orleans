
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using DotNetty.Transport.Channels;

namespace Orleans.Runtime.Messaging
{
    internal class SiloMessageSender : AsynchQueueAgent<Message>
    {
        private readonly MessageCenter messageCenter;
        private const int DEFAULT_MAX_RETRIES = 0;
        private readonly Dictionary<SiloAddress, DateTime> lastConnectionFailure;

        internal const string RETRY_COUNT_TAG = "RetryCount";
        internal static readonly TimeSpan CONNECTION_RETRY_DELAY = TimeSpan.FromMilliseconds(1000);
        
        internal SiloMessageSender(string nameSuffix, MessageCenter msgCtr)
            : base(nameSuffix, msgCtr.MessagingConfiguration)
        {
            messageCenter = msgCtr;
            lastConnectionFailure = new Dictionary<SiloAddress, DateTime>();

            OnFault = FaultBehavior.RestartOnFault;
        }

        private bool PrepareMessageForSend(Message msg)
        {
            // Don't send messages that have already timed out
            if (msg.IsExpired)
            {
                msg.DropExpiredMessage(MessagingStatisticsGroup.Phase.Send);
                return false;
            }

            // Fill in the outbound message with our silo address, if it's not already set
            if (!msg.ContainsHeader(Message.Header.SENDING_SILO))
                msg.SendingSilo = messageCenter.MyAddress;
            

            // If there's no target silo set, then we shouldn't see this message; send it back
            if (msg.TargetSilo == null)
            {
                FailMessage(msg, "No target silo provided -- internal error");
                return false;
            }

            // If we know this silo is dead, don't bother
            if ((messageCenter.SiloDeadOracle != null) && messageCenter.SiloDeadOracle(msg.TargetSilo))
            {
                FailMessage(msg, $"Target {msg.TargetSilo.ToLongString()} silo is known to be dead");
                return false;
            }

            // If we had a bad connection to this address recently, don't even try
            DateTime failure;
            if (lastConnectionFailure.TryGetValue(msg.TargetSilo, out failure))
            {
                var since = DateTime.UtcNow.Subtract(failure);
                if (since < CONNECTION_RETRY_DELAY)
                {
                    FailMessage(msg,
                        $"Recent ({since} ago, at {TraceLogger.PrintDate(failure)}) connection failure trying to reach target silo {msg.TargetSilo.ToLongString()}. Going to drop {msg.Direction} msg {msg.Id} without sending. CONNECTION_RETRY_DELAY = {CONNECTION_RETRY_DELAY}.");
                    return false;
                }
            }

            return true;
        }

        private async Task<IChannel> GetSendingChannel(Message msg)
        {
            var targetSilo = msg.TargetSilo;
            try
            {
                IChannel channel = await messageCenter.ChannelManager.GetChannel(targetSilo.Endpoint);
                if (channel.Open && channel.Active) return channel;

                messageCenter.ChannelManager.InvalidateEntry(targetSilo.Endpoint);
                return await messageCenter.ChannelManager.GetChannel(targetSilo.Endpoint);
            }
            catch (Exception ex)
            {
                var error = "Exception getting a sending channel to endpoint " + targetSilo;
                Log.Warn(ErrorCode.Messaging_UnableToGetSendingChannel, error, ex);
                messageCenter.ChannelManager.InvalidateEntry(targetSilo.Endpoint);
                lastConnectionFailure[targetSilo] = DateTime.UtcNow;
                throw;
            }
        }

        private void OnGetSendingFailure(Message msg, string error)
        {
            FailMessage(msg, error);
        }

        private void OnSendFailure(SiloAddress targetSilo)
        {
            messageCenter.ChannelManager.InvalidateEntry(targetSilo.Endpoint);
        }

        private void ProcessMessageAfterSend(Message msg, bool sendError)
        {
            if (sendError)
            {
                msg.ReleaseHeadersOnly();
                RetryMessage(msg);
            }
            else
            {
                msg.ReleaseBodyAndHeaderBuffers();
                if (Log.IsVerbose3) Log.Verbose3("Sending queue delay time for: {0} is {1}", msg, DateTime.UtcNow.Subtract((DateTime)msg.GetMetadata(OutboundMessageQueue.QUEUED_TIME_METADATA)));
            }
        }

        private void FailMessage(Message msg, string reason)
        {
            msg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(msg);
            if (msg.Direction == Message.Directions.Request)
            {
                if (Log.IsVerbose) Log.Verbose(ErrorCode.MessagingSendingRejection, "Silo {0} is rejecting message: {0}. Reason = {1}", messageCenter.MyAddress, msg, reason);
                // Done retrying, send back an error instead
                messageCenter.SendRejection(msg, Message.RejectionTypes.Transient,
                    $"Silo {messageCenter.MyAddress} is rejecting message: {msg}. Reason = {reason}");
            }else
            {
                Log.Info(ErrorCode.Messaging_OutgoingMS_DroppingMessage, "Silo {0} is dropping message: {0}. Reason = {1}", messageCenter.MyAddress, msg, reason);
                MessagingStatisticsGroup.OnDroppedSentMessage(msg);
            }
        }

        private void RetryMessage(Message msg, Exception ex = null)
        {
            if (msg == null) return;

            int maxRetries = DEFAULT_MAX_RETRIES;
            if (msg.ContainsMetadata(Message.Metadata.MAX_RETRIES))
                maxRetries = (int)msg.GetMetadata(Message.Metadata.MAX_RETRIES);
            
            int retryCount = 0;
            if (msg.ContainsMetadata(RETRY_COUNT_TAG))
                retryCount = (int)msg.GetMetadata(RETRY_COUNT_TAG);
            
            if (retryCount < maxRetries)
            {
                msg.SetMetadata(RETRY_COUNT_TAG, retryCount + 1);
                messageCenter.OutboundQueue.SendMessage(msg);
            }
            else
            {
                var reason = new StringBuilder("Retry count exceeded. ");
                if (ex != null)
                {
                    reason.Append("Original exception is: ").Append(ex);
                }
                reason.Append("Msg is: ").Append(msg);
                FailMessage(msg, reason.ToString());
            }
        }

        protected override void Process(Message request)
        {
            ProcessAsync(request).Wait();
        }

        private async Task ProcessAsync(Message request)
        {
            if (Log.IsVerbose2) Log.Verbose2("Got a {0} message to send: {1}", request.Direction, request);
            bool continueSend = PrepareMessageForSend(request);
            if (!continueSend) return;

            IChannel channel;
            SiloAddress targetSilo = request.TargetSilo;
            try
            {
                channel = await GetSendingChannel(request);
            }
            catch (Exception ex)
            {
                OnGetSendingFailure(request, ex.ToString());
                return;
            }

            bool exceptionSending = false;
            try
            {
                await channel.WriteAndFlushAsync(request);
            }
            catch (Exception exc)
            {
                exceptionSending = true;
                string sendErrorStr = $"Exception sending message to {targetSilo}. Message: {request}. {exc}";
                Log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
            }
            if (exceptionSending)
                OnSendFailure(targetSilo);

            ProcessMessageAfterSend(request, exceptionSending);
        }
    }
}
