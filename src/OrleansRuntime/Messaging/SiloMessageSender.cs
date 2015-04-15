/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using Orleans.Messaging;

namespace Orleans.Runtime.Messaging
{
    internal class SiloMessageSender : OutgoingMessageSender
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

        protected override SocketDirection GetSocketDirection()
        {
            return SocketDirection.SiloToSilo;
        }

        protected override bool PrepareMessageForSend(Message msg)
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
                FailMessage(msg, String.Format("Target {0} silo is known to be dead", msg.TargetSilo.ToLongString()));
                return false;
            }

            // If we had a bad connection to this address recently, don't even try
            DateTime failure;
            if (lastConnectionFailure.TryGetValue(msg.TargetSilo, out failure))
            {
                var since = DateTime.UtcNow.Subtract(failure);
                if (since < CONNECTION_RETRY_DELAY)
                {
                    FailMessage(msg, String.Format("Recent ({0} ago, at {1}) connection failure trying to reach target silo {2}. Going to drop {3} msg {4} without sending. CONNECTION_RETRY_DELAY = {5}.",
                        since, TraceLogger.PrintDate(failure), msg.TargetSilo.ToLongString(), msg.Direction, msg.Id, CONNECTION_RETRY_DELAY));
                    return false;
                }
            }

            if (Message.WriteMessagingTraces)
                msg.AddTimestamp(Message.LifecycleTag.SendOutgoing);

            return true;
        }

        protected override bool GetSendingSocket(Message msg, out SocketSender socket, out SiloAddress targetSilo, out string error)
        {
            socket = null;
            targetSilo = msg.TargetSilo;
            error = null;
            try
            {
                socket = messageCenter.SocketManager.GetSendingSocket(targetSilo.Endpoint);
                if (socket.MySocket.Connected) return true;

                messageCenter.SocketManager.InvalidateEntry(targetSilo.Endpoint);
                socket = messageCenter.SocketManager.GetSendingSocket(targetSilo.Endpoint);
                return true;
            }
            catch (Exception ex)
            {
                error = "Exception getting a sending socket to endpoint " + targetSilo.ToString();
                Log.Warn(ErrorCode.Messaging_UnableToGetSendingSocket, error, ex);
                messageCenter.SocketManager.InvalidateEntry(targetSilo.Endpoint);
                lastConnectionFailure[targetSilo] = DateTime.UtcNow;
                return false;
            }
        }

        protected override void OnGetSendingSocketFailure(Message msg, string error)
        {
            FailMessage(msg, error);
        }

        protected override void OnMessageSerializationFailure(Message msg, Exception exc)
        {
            // we only get here if we failed to serialize the msg (or any other catastrophic failure).
            // Request msg fails to serialize on the sending silo, so we just enqueue a rejection msg.
            // Response msg fails to serialize on the responding silo, so we try to send an error response back.
            Log.Warn(ErrorCode.MessagingUnexpectedSendError, String.Format("Unexpected error sending message {0}", msg.ToString()), exc);
            
            msg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(msg);
            if (msg.Direction == Message.Directions.Request)
            {
                messageCenter.SendRejection(msg, Message.RejectionTypes.Unrecoverable, exc.ToString());
            }
            else if (msg.Direction == Message.Directions.Response && msg.Result != Message.ResponseTypes.Error)
            {
                // if we failed sending an original response, turn the response body into an error and reply with it.
                // unless the response was already an error response (so we don't loop forever).
                msg.Result = Message.ResponseTypes.Error;
                msg.BodyObject = Response.ExceptionResponse(exc);
                messageCenter.SendMessage(msg);
            }
            else
            {
                MessagingStatisticsGroup.OnDroppedSentMessage(msg);
            }
        }

        protected override void OnSendFailure(Socket socket, SiloAddress targetSilo)
        {
            messageCenter.SocketManager.InvalidateEntry(targetSilo.Endpoint);
        }

        protected override void ProcessMessageAfterSend(Message msg, bool sendError, string sendErrorStr)
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

        protected override void FailMessage(Message msg, string reason)
        {
            msg.ReleaseBodyAndHeaderBuffers();
            MessagingStatisticsGroup.OnFailedSentMessage(msg);
            if (msg.Direction == Message.Directions.Request)
            {
                if (Log.IsVerbose) Log.Verbose(ErrorCode.MessagingSendingRejection, "Silo {0} is rejecting message: {0}. Reason = {1}", messageCenter.MyAddress, msg, reason);
                // Done retrying, send back an error instead
                messageCenter.SendRejection(msg, Message.RejectionTypes.Transient, String.Format("Silo {0} is rejecting message: {1}. Reason = {2}", messageCenter.MyAddress, msg, reason));
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
                    reason.Append("Original exception is: ").Append(ex.ToString());
                }
                reason.Append("Msg is: ").Append(msg);
                FailMessage(msg, reason.ToString());
            }
        }
    }
}
