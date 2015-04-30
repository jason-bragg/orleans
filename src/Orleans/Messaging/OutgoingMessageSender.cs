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
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;


namespace Orleans.Messaging
{
    internal enum SocketDirection
    {
        SiloToSilo,
        ClientToGateway,
        GatewayToClient
    }

    internal abstract class OutgoingMessageSender : AsynchQueueAgent<Message>
    {
        private readonly SocketAsyncEventArgs sendSocketEvent;
        private readonly ManualResetEvent doneEvent;

        private class SendState
        {
            public Message Message { get; set; }
//            public List<Message> Messages{ get; set; }
            public List<ArraySegment<byte>> Data { get; set; }
            public SiloAddress TargetSilo { get; set; }
            public Message.Directions Direction { get; set; }
            public int HeaderLength { get; set; }
            public int Length { get; set; }
            public int Sent { get; set; }
        }

        private readonly SendState state;

        internal OutgoingMessageSender(string nameSuffix, IMessagingConfiguration config)
            : base(nameSuffix, config)
        {
            doneEvent = new ManualResetEvent(false);
            state = new SendState();
            sendSocketEvent = new SocketAsyncEventArgs();
            sendSocketEvent.Completed += OnSendCompleted;
        }

        protected override void Process(Message msg)
        {
            doneEvent.Reset();
            Process_internal(msg);
            doneEvent.WaitOne();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void Process_internal(Message msg)
        {
            bool sendComplete = true;

            try
            {
                sendComplete = SendAsync(msg);
            }
            catch (Exception exc)
            {
                string sendErrorStr = String.Format("Unhandled exception on async send. TargetSilo {0}. Exception: {1}", state.TargetSilo, exc);
                Log.Error(ErrorCode.Messaging_OutgoingMS_ProcessMsgError, sendErrorStr, exc);
            }

            if (sendComplete)
            {
                ProcessNext();
            }
        }

        // Return true if send completed and we should move to next message, false if it's still pending.
        private bool SendAsync(Message msg)
        {
            if (Log.IsVerbose2) Log.Verbose2("Got a {0} message to send: {1}", msg.Direction, msg);
            bool continueSend = PrepareMessageForSend(msg);
            if (!continueSend)
            {
                return true;
            }

            Socket sock;
            string error;
            SiloAddress targetSilo;
            continueSend = GetSendingSocket(msg, out sock, out targetSilo, out error);
            if (!continueSend)
            {
                OnGetSendingSocketFailure(msg, error);
                return true;
            }

            int headerLength;
            try
            {
                state.Data = msg.Serialize(out headerLength);
            }
            catch (Exception exc)
            {
                OnMessageSerializationFailure(msg, exc);
                return true;
            }

            state.Message = msg;
            state.TargetSilo = targetSilo;
            state.Direction = msg.Direction;
            state.HeaderLength = headerLength;
            state.Length = state.Data.Sum(x => x.Count);
            state.Sent = 0;
            try
            {
                sendSocketEvent.BufferList = state.Data;
                if (!sock.SendAsync(sendSocketEvent))
                {
                    return TryComplete(sock, sendSocketEvent);
                }
                return false;
            }
            catch (Exception exc)
            {
                string sendErrorStr = String.Empty;
                if (!(exc is ObjectDisposedException))
                {
                    sendErrorStr = String.Format("Exception sending message to {0}. Message: {1}. {2}", targetSilo, msg, exc);
                    Log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                }
                ProcessMessageAfterSend(state.Message, true, sendErrorStr);
                OnSendFailure(sock, targetSilo);
                return true;
            }
        }

        private void ProcessNext()
        {
            Message message;
            if (!requestQueue.TryTake(out message))
            {
                doneEvent.Set();
                return;
            }

            Process_internal(message);
        }

        private void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
            bool sendComplete = true;

            try
            {
                sendComplete = TryComplete(sender, e);
            }
            catch (Exception exc)
            {
                string sendErrorStr = String.Format("Unhandled exception on send complete. TargetSilo {0}. Exception: {1}", state.TargetSilo, exc);
                Log.Error(ErrorCode.Messaging_OutgoingMS_SendCompleteError, sendErrorStr, exc);
            }

            if (sendComplete)
            {
                ProcessNext();
            }
        }

        // return true if send completed and we should move to next message.  false if send is incomplete
        private bool TryComplete(object sender, SocketAsyncEventArgs e)
        {
            var sock = (Socket)sender;
            state.Sent += e.BytesTransferred;
            string sendErrorStr = String.Empty;

            if (e.SocketError == SocketError.Success)
            {
                if (state.Sent == state.Length)
                {
                    // message sent, process next message
                    MessagingStatisticsGroup.OnMessageSend(state.TargetSilo, state.Direction, state.Length, state.HeaderLength, GetSocketDirection());
                    ProcessMessageAfterSend(state.Message, false, string.Empty);
                    return true;
                }

                try
                {
                    // send incomplete, send the rest
                    e.BufferList = ByteArrayBuilder.BuildSegmentList(state.Data, state.Sent);
                    if (!sock.SendAsync(e))
                    {
                        return TryComplete(sender, e);
                    }
                    return false;
                }
                catch (Exception exc)
                {
                    if (!(exc is ObjectDisposedException))
                    {
                        sendErrorStr = String.Format("Exception continueing to send message to {0}. {1}", state.TargetSilo, exc);
                        Log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                    }
                }
            }

            // something went wrong
            ProcessMessageAfterSend(state.Message, true, sendErrorStr);
            OnSendFailure(sock, state.TargetSilo);
            return true;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected override void ProcessBatch(List<Message> msgs)
        {
            /*
            doneEvent.Reset();
            ProcessBatch_internal(msgs);
            doneEvent.WaitOne();
             */
        }

        /*
        private void ProcessBatch_internal(List<Message> msgs)
        {
            if (Log.IsVerbose2) Log.Verbose2("Got {0} messages to send.", msgs.Count);
            for (int i = 0; i < msgs.Count; )
            {
                bool sendThisMessage = PrepareMessageForSend(msgs[i]);
                if (sendThisMessage)
                    i++;
                else
                    msgs.RemoveAt(i); // don't advance i
            }

            if (msgs.Count <= 0)
            {
                doneEvent.Set();
                return;
            }

            Socket sock;
            string error;
            SiloAddress targetSilo;
            bool continueSend = GetSendingSocket(msgs[0], out sock, out targetSilo, out error);
            if (!continueSend)
            {
                foreach (Message msg in msgs)
                    OnGetSendingSocketFailure(msg, error);
                doneEvent.Set();
                return;
            }

            List<ArraySegment<byte>> data;
            int headerLength;
            continueSend = SerializeMessages(msgs, out data, out headerLength, OnMessageSerializationFailure);
            if (!continueSend)
            {
                doneEvent.Set();
                return;
            }

            state.Messages = msgs;
            state.TargetSilo = targetSilo;
            state.Direction = msgs[0].Direction;
            state.HeaderLength = headerLength;
            state.Length = state.Data.Sum(x => x.Count);
            state.Sent = 0;
            try
            {
                sendSocketEvent.BufferList = state.Data;
                if (!sock.SendAsync(sendSocketEvent))
                {
                    OnBatchSendCompleted(sock, sendSocketEvent);
                }
            }
            catch (Exception exc)
            {
                if (!(exc is ObjectDisposedException))
                {
                    string sendErrorStr = String.Format("Exception sending message to {0}. {1}", targetSilo, TraceLogger.PrintException(exc));
                    Log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                }
                OnSendFailure(sock, targetSilo);
                doneEvent.Set();
            }
        }
        
        private void OnBatchSendCompleted(object sender, SocketAsyncEventArgs e)
        {
            var sock = (Socket)sender;
            state.Sent += e.BytesTransferred;
            string sendErrorStr = String.Empty;

            if (e.SocketError == SocketError.Success)
            {
                if (state.Sent == state.Length)
                {
                    // batch of messages send, we're done here
                    MessagingStatisticsGroup.OnMessageBatchSend(state.TargetSilo, state.Direction, state.Length, state.HeaderLength, GetSocketDirection(), state.Messages.Count);
                    foreach (Message msg in state.Messages)
                    {
                        ProcessMessageAfterSend(msg, false, string.Empty);
                    }
                    doneEvent.Set();
                    return;
                }

                try
                {
                    // send incomplete, send the rest
                    e.BufferList = ByteArrayBuilder.BuildSegmentList(state.Data, state.Sent);
                    if (!sock.SendAsync(e))
                    {
                        OnBatchSendCompleted(sender, e);
                    }
                    return;
                }
                catch (Exception exc)
                {
                    if (!(exc is ObjectDisposedException))
                    {
                        sendErrorStr = String.Format("Exception continueing to send message to {0}. {1}", state.TargetSilo, exc);
                        Log.Warn(ErrorCode.Messaging_ExceptionSending, sendErrorStr, exc);
                    }
                }
            }

            foreach (Message msg in state.Messages)
            {
                ProcessMessageAfterSend(msg, true, sendErrorStr);
            }

            OnSendFailure(sock, state.TargetSilo);
            doneEvent.Set();
        }
        */

        public static bool SerializeMessages(List<Message> msgs, out List<ArraySegment<byte>> data, out int headerLengthOut, Action<Message, Exception> msgSerializationFailureHandler)
        {
            int numberOfValidMessages = 0;
            var lengths = new List<ArraySegment<byte>>();
            var bodies = new List<ArraySegment<byte>>();
            data = null;
            headerLengthOut = 0;
            int totalHeadersLen = 0;

            foreach (var message in msgs)
            {
                try
                {
                    int headerLength;
                    int bodyLength;
                    List<ArraySegment<byte>> body = message.SerializeForBatching(out headerLength, out bodyLength);
                    var headerLen = new ArraySegment<byte>(BitConverter.GetBytes(headerLength));
                    var bodyLen = new ArraySegment<byte>(BitConverter.GetBytes(bodyLength));

                    bodies.AddRange(body);
                    lengths.Add(headerLen);
                    lengths.Add(bodyLen);
                    numberOfValidMessages++;
                    totalHeadersLen += headerLength;
                }
                catch (Exception exc)
                {
                    if (msgSerializationFailureHandler != null)
                        msgSerializationFailureHandler(message, exc);
                    else
                        throw;
                }
            }

            // at least 1 message was successfully serialized
            if (bodies.Count <= 0) return false;

            data = new List<ArraySegment<byte>> { new ArraySegment<byte>(BitConverter.GetBytes(numberOfValidMessages)) };
            data.AddRange(lengths);
            data.AddRange(bodies);
            headerLengthOut = totalHeadersLen;
            return true;
            // no message serialized
        }

        protected abstract SocketDirection GetSocketDirection();
        protected abstract bool PrepareMessageForSend(Message msg);
        protected abstract bool GetSendingSocket(Message msg, out Socket socket, out SiloAddress targetSilo, out string error);
        protected abstract void OnGetSendingSocketFailure(Message msg, string error);
        protected abstract void OnMessageSerializationFailure(Message msg, Exception exc);
        protected abstract void OnSendFailure(Socket socket, SiloAddress targetSilo);
        protected abstract void ProcessMessageAfterSend(Message msg, bool sendError, string sendErrorStr);
        protected abstract void FailMessage(Message msg, string reason);
    }
}