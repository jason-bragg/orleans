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
        private readonly byte[] buffer;
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
            buffer = new byte[bufferSize];
            sendSocketEvent = new SocketAsyncEventArgs();
            sendSocketEvent.SetBuffer(buffer, 0, 0);
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

            int size = FillBuffer();
            WriteBuffer(size);
        }

        private void WriteBuffer(int size)
        {
            sendSocketEvent.SetBuffer(0, size);
            bool willRaiseEvent = MySocket.SendAsync(sendSocketEvent);

            if (!willRaiseEvent)
            {
                Send();
            }
        }

        private int FillBuffer()
        {
            int writeOffset = 0;
            while (!pendingMessages.IsEmpty)
            {
                List<ArraySegment<byte>> message;
                if (!pendingMessages.TryPeek(out message))
                    break;
                if (message.Sum(seg => seg.Count) + writeOffset >= DefaultBufferSize)
                    break;
                if (!pendingMessages.TryDequeue(out message))
                    break;
                foreach (ArraySegment<byte> segment in message)
                {
                    Buffer.BlockCopy(segment.Array, segment.Offset, buffer, writeOffset, segment.Count);
                    writeOffset += segment.Count;
                }
                BufferPool.GlobalPool.Release(message);
                if (!Batching)
                    break;
            }
            return writeOffset;
        }

        private void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
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
