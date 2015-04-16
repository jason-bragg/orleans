using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Threading;

namespace Orleans.Runtime
{
    public class SocketSender
    {
        private const int DefaultBufferSize = 1 << 18; // 256k
        private readonly SocketAsyncEventArgs sendSocketEvent;
        private readonly ConcurrentQueue<List<ArraySegment<byte>>> pendingMessages;
        private readonly List<ArraySegment<byte>> messageList;
        private readonly int bufferSize;
        private int active;

        public Socket MySocket { get; private set; }
        public bool Batching { get; set; }

        public SocketSender(Socket socket, int bufferSize = DefaultBufferSize)
        {
            if (socket == null)
            {
                throw new ArgumentNullException("socket");
            }
            MySocket = socket;
            this.bufferSize = bufferSize;
            sendSocketEvent = new SocketAsyncEventArgs();
            sendSocketEvent.BufferList = messageList;
            messageList = new List<ArraySegment<byte>>();
            sendSocketEvent.Completed += OnSendCompleted;
            pendingMessages = new ConcurrentQueue<List<ArraySegment<byte>>>();
            Batching = true;
        }

        public void Send(List<ArraySegment<byte>> data)
        {
            pendingMessages.Enqueue(data);
            if (0==Interlocked.CompareExchange(ref active, 1, 0))
            {
                Send();
            }
        }

        private void Send()
        {
            if (pendingMessages.IsEmpty)
            {
                active = 0;
                return;
            }

            FillBuffer();
            WriteBuffer();
        }

        private void WriteBuffer()
        {
            if (!MySocket.SendAsync(sendSocketEvent))
            {
                OnSendCompleted(MySocket, sendSocketEvent);
            }
        }

        private void FillBuffer()
        {
            int sendSize = 0;
            messageList.Clear();
            while (!pendingMessages.IsEmpty)
            {
                List<ArraySegment<byte>> message;
                if (!pendingMessages.TryPeek(out message))
                    break;
                sendSize += message.Sum(seg => seg.Count);
                if (sendSize >= bufferSize)
                    break;
                if (!pendingMessages.TryDequeue(out message))
                    break;
                messageList.AddRange(message);
                if (!Batching)
                    break;
            }
        }

        private void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
            BufferPool.GlobalPool.Release(e.BufferList);

            Send();
        }

        private void OnError()
        {
/*            if (bytesSent != length)
            {
                // The complete message wasn't sent, even though no error was reported; treat this as an error
                countMismatchSending = true;
                sendErrorStr = String.Format("Byte count mismatch on sending to {0}: sent {1}, expected {2}", targetSilo, bytesSent, length);
                Log.Warn(ErrorCode.Messaging_CountMismatchSending, sendErrorStr);
            }
 */
        }
    }
}
