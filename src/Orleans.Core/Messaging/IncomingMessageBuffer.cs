using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    internal class IncomingMessageBuffer
    {
        private const int Kb = 1024;
        private const int DEFAULT_MAX_SUSTAINED_RECEIVE_BUFFER_SIZE = 1024 * Kb; // 1mg
        private const int GROW_MAX_BLOCK_SIZE = 1024 * Kb; // 1mg
        private static readonly ArraySegment<byte>[] EmptyBuffers = {new ArraySegment<byte>(new byte[0]), };
        private readonly DriftingLinkedList readBuffer;
        private readonly int maxSustainedBufferSize;
        private int currentBufferSize;

        private readonly byte[] lengthBuffer;

        private int headerLength;
        private int bodyLength;

        private int receiveOffset;
        private int decodeOffset;

        private readonly bool supportForwarding;
        private ILogger Log;
        private readonly SerializationManager serializationManager;
        private readonly DeserializationContext deserializationContext;

        internal const int DEFAULT_RECEIVE_BUFFER_SIZE = 128 * Kb; // 128k

        public IncomingMessageBuffer(
            ILoggerFactory loggerFactory,
            SerializationManager serializationManager,
            bool supportForwarding = false,
            int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE,
            int maxSustainedReceiveBufferSize = DEFAULT_MAX_SUSTAINED_RECEIVE_BUFFER_SIZE)
        {
            Log = loggerFactory.CreateLogger<IncomingMessageBuffer>();
            this.serializationManager = serializationManager;
            this.supportForwarding = supportForwarding;
            currentBufferSize = receiveBufferSize;
            maxSustainedBufferSize = maxSustainedReceiveBufferSize;
            lengthBuffer = new byte[Message.LENGTH_HEADER_SIZE];
            readBuffer = new DriftingLinkedList(BufferPool.GlobalPool.GetMultiBuffer(currentBufferSize));
            receiveOffset = 0;
            decodeOffset = 0;
            headerLength = 0;
            bodyLength = 0;
            deserializationContext = new DeserializationContext(this.serializationManager)
            {
                StreamReader = new BinaryTokenStreamReader(EmptyBuffers)
            };
        }

        public IList<ArraySegment<byte>> BuildReceiveBuffer()
        {
            // Opportunistic reset to start of buffer
            if (decodeOffset == receiveOffset)
            {
                decodeOffset = 0;
                receiveOffset = 0;
            }
            return readBuffer.After(receiveOffset);
        }

        // Copies receive buffer into read buffer for futher processing
        public void UpdateReceivedData(byte[] receiveBuffer, int bytesRead)
        {
            var newReceiveOffset = receiveOffset + bytesRead;
            while (newReceiveOffset > currentBufferSize)
            {
                GrowBuffer();
            }

            int receiveBufferOffset = 0;
            var lengthSoFar = 0;

            foreach (var segment in readBuffer)
            {
                var bytesStillToSkip = receiveOffset - lengthSoFar;
                lengthSoFar += segment.Count;

                if (segment.Count <= bytesStillToSkip)
                {
                    continue;
                }

                if(bytesStillToSkip > 0) // This is the first buffer
                {
                    var bytesToCopy = Math.Min(segment.Count - bytesStillToSkip, bytesRead - receiveBufferOffset);
                    Buffer.BlockCopy(receiveBuffer, receiveBufferOffset, segment.Array, bytesStillToSkip, bytesToCopy);
                    receiveBufferOffset += bytesToCopy;
                }
                else
                {
                    var bytesToCopy = Math.Min(segment.Count, bytesRead - receiveBufferOffset);
                    Buffer.BlockCopy(receiveBuffer, receiveBufferOffset, segment.Array, 0, bytesToCopy);
                    receiveBufferOffset += Math.Min(bytesToCopy, segment.Count);
                }

                if (receiveBufferOffset == bytesRead)
                {
                    break;
                }
            }

            receiveOffset += bytesRead;
        }

        public void UpdateReceivedData(int bytesRead)
        {
            receiveOffset += bytesRead;
        }

        public void Reset()
        {
            receiveOffset = 0;
            decodeOffset = 0;
            headerLength = 0;
            bodyLength = 0;
        }

        public bool TryDecodeMessage(out Message msg)
        {
            msg = null;

            // Is there enough read into the buffer to continue (at least read the lengths?)
            if (receiveOffset - decodeOffset < CalculateKnownMessageSize())
                return false;

            // parse lengths if needed
            if (headerLength == 0 || bodyLength == 0)
            {
                // get length segments
                List<ArraySegment<byte>> lenghts = ByteArrayBuilder.BuildSegmentListWithLengthLimit(readBuffer, decodeOffset, Message.LENGTH_HEADER_SIZE);

                // copy length segment to buffer
                int lengthBufferoffset = 0;
                foreach (ArraySegment<byte> seg in lenghts)
                {
                    Buffer.BlockCopy(seg.Array, seg.Offset, lengthBuffer, lengthBufferoffset, seg.Count);
                    lengthBufferoffset += seg.Count;
                }

                // read lengths
                headerLength = BitConverter.ToInt32(lengthBuffer, 0);
                bodyLength = BitConverter.ToInt32(lengthBuffer, 4);
            }

            // If message is too big for current buffer size, grow
            while (decodeOffset + CalculateKnownMessageSize() > currentBufferSize)
            {
                GrowBuffer();
            }

            // Is there enough read into the buffer to read full message
            if (receiveOffset - decodeOffset < CalculateKnownMessageSize())
                return false;

            // decode header
            int headerOffset = decodeOffset + Message.LENGTH_HEADER_SIZE;
            List<ArraySegment<byte>> header = ByteArrayBuilder.BuildSegmentListWithLengthLimit(readBuffer, headerOffset, headerLength);

            // decode body
            int bodyOffset = headerOffset + headerLength;
            List<ArraySegment<byte>> body = ByteArrayBuilder.BuildSegmentListWithLengthLimit(readBuffer, bodyOffset, bodyLength);
            
            // build message

            this.deserializationContext.Reset();
            this.deserializationContext.StreamReader.Reset(header);

            msg = new Message
            {
                Headers = SerializationManager.DeserializeMessageHeaders(this.deserializationContext)
            };
            try
            {
                if (this.supportForwarding)
                {
                    // If forwarding is supported, then deserialization will be deferred until the body value is needed.
                    // Need to maintain ownership of buffer, so we need to duplicate the body buffer.
                    msg.SetBodyBytes(this.DuplicateBuffer(body));
                }
                else
                {
                    // Attempt to deserialize the body immediately.
                    msg.DeserializeBodyObject(this.serializationManager, body);
                }
            }
            finally
            {
                MessagingStatisticsGroup.OnMessageReceive(msg, headerLength, bodyLength);

                if (headerLength + bodyLength > this.serializationManager.LargeObjectSizeThreshold)
                {
                    Log.Info(
                        ErrorCode.Messaging_LargeMsg_Incoming,
                        "Receiving large message Size={0} HeaderLength={1} BodyLength={2}. Msg={3}",
                        headerLength + bodyLength,
                        headerLength,
                        bodyLength,
                        msg.ToString());
                    if (Log.IsEnabled(LogLevel.Trace)) Log.Trace("Received large message {0}", msg.ToLongString());
                }

                // update parse receiveOffset and clear lengths
                decodeOffset = bodyOffset + bodyLength;
                headerLength = 0;
                bodyLength = 0;

                AdjustBuffer();
            }

            return true;
        }

        /// <summary>
        /// This call cleans up the buffer state to make it optimal for next read.
        /// The leading chunks, used by any processed messages, are removed from the front
        ///   of the buffer and added to the back.   Decode and receiver offsets are adjusted accordingly.
        /// If the buffer was grown over the max sustained buffer size (to read a large message) it is shrunken.
        /// </summary>
        private void AdjustBuffer()
        {
            // drop buffers consumed by messages and adjust offsets
            // TODO: This can be optimized further. Linked lists?
            int consumedBytes = 0;
            while (readBuffer.Count != 0)
            {
                ArraySegment<byte> seg = readBuffer[0];
                if (seg.Count <= decodeOffset - consumedBytes)
                {
                    consumedBytes += seg.Count;
                    readBuffer.Remove(seg);
                    BufferPool.GlobalPool.Release(seg.Array);
                }
                else
                {
                    break;
                }
            }
            decodeOffset -= consumedBytes;
            receiveOffset -= consumedBytes;

            // backfill any consumed buffers, to preserve buffer size.
            if (consumedBytes != 0)
            {
                int backfillBytes = consumedBytes;
                // If buffer is larger than max sustained size, backfill only up to max sustained buffer size.
                if (currentBufferSize > maxSustainedBufferSize)
                {
                    backfillBytes = Math.Max(consumedBytes + maxSustainedBufferSize - currentBufferSize, 0);
                    currentBufferSize -= consumedBytes;
                    currentBufferSize += backfillBytes;
                }
                if (backfillBytes > 0)
                {
                    readBuffer.AddRange(BufferPool.GlobalPool.GetMultiBuffer(backfillBytes));
                }
            }
        }

        private int CalculateKnownMessageSize()
        {
            return headerLength + bodyLength + Message.LENGTH_HEADER_SIZE;
        }

        private List<ArraySegment<byte>> DuplicateBuffer(List<ArraySegment<byte>> body)
        {
            var dupBody = new List<ArraySegment<byte>>(body.Count);
            foreach (ArraySegment<byte> seg in body)
            {
                var dupSeg = new ArraySegment<byte>(BufferPool.GlobalPool.GetBuffer(), seg.Offset, seg.Count);
                Buffer.BlockCopy(seg.Array, seg.Offset, dupSeg.Array, dupSeg.Offset, seg.Count);
                dupBody.Add(dupSeg);
            }
            return dupBody;
        }

        private void GrowBuffer()
        {
            //TODO: Add configurable max message size for safety
            //TODO: Review networking layer and add max size checks to all dictionaries, arrays, or other variable sized containers.
            // double buffer size up to max grow block size, then only grow it in those intervals
            int growBlockSize = Math.Min(currentBufferSize, GROW_MAX_BLOCK_SIZE);
            readBuffer.AddRange(BufferPool.GlobalPool.GetMultiBuffer(growBlockSize));
            currentBufferSize += growBlockSize;
        }

        /// <summary>
        /// Linked list of array segments rooted at the last item accessed.
        /// </summary>
        private class DriftingLinkedList : IList<ArraySegment<byte>>
        {
            private class Node
            {
                public ArraySegment<byte> segment;
                public Node prev;
                public Node next;
            }
            private Node root;
            private int count;
            private int idx;

            public DriftingLinkedList(IList<ArraySegment<byte>> segments)
            {
                AddRange(segments);
            }

            public IList<ArraySegment<byte>> After(int offset)
            {
                if (offset == 0)
                {
                    return this;
                }

                var result = new List<ArraySegment<byte>>();
                var lengthSoFar = 0;
                foreach (var segment in this)
                {
                    var bytesStillToSkip = offset - lengthSoFar;
                    lengthSoFar += segment.Count;
                    if (segment.Count <= bytesStillToSkip) // Still skipping past this buffer
                    {
                        continue;
                    }
                    if (bytesStillToSkip > 0) // This is the first buffer, so just take part of it
                    {
                        result.Add(new ArraySegment<byte>(segment.Array, bytesStillToSkip, segment.Count - bytesStillToSkip));
                    }
                    else // Take the whole buffer
                    {
                        result.Add(segment);
                    }
                }
                return result;
            }

            public void AddRange(IList<ArraySegment<byte>> items)
            {
                foreach (ArraySegment<byte> item in items)
                {
                    Add(item);
                }
            }

            public bool IsFixedSize => false;

            public bool IsReadOnly => false;

            public int Count => this.count;

            public bool IsSynchronized => false;

            public object syncRoot = new object();
            public object SyncRoot => this.syncRoot;

            public ArraySegment<byte> this[int index]
            {
                get
                {
                    Node found = this.root;
                    int offset = this.idx;
                    if (index >= offset)
                        while (index >= offset && found != null)
                        {
                            if (index == offset)
                            {
                                this.idx = offset;
                                this.root = found;
                                return this.root.segment;
                            }
                            offset++;
                            found = found.next;
                        }
                    else if (index <= offset)
                        while (index <= offset && found != null)
                        {
                            if (index == offset)
                            {
                                this.idx = offset;
                                this.root = found;
                                return this.root.segment;
                            }
                            offset--;
                            found = found.prev;
                        }
                    throw new ArgumentOutOfRangeException(nameof(index));
                }
                set
                {
                    Node found = this.root;
                    int offset = this.idx;
                    if (index >= offset)
                        while (index >= offset && found != null)
                        {
                            if (index == offset)
                            {
                                this.idx = offset;
                                this.root = found;
                                this.root.segment = value;
                            }
                            offset++;
                            found = found.next;
                        }
                    else if (index <= offset)
                        while (index <= offset && found != null)
                        {
                            if (index == offset)
                            {
                                this.idx = offset;
                                this.root = found;
                                this.root.segment = value;
                            }
                            offset--;
                            found = found.prev;
                        }
                    throw new ArgumentOutOfRangeException(nameof(index));
                }
            }

            public void Clear()
            {
                this.root = null;
                this.count = 0;
                this.idx = 0;
            }

            public IEnumerator<ArraySegment<byte>> GetEnumerator()
            {
                throw new NotImplementedException();
            }

            public void RemoveAt(int index)
            {
                throw new NotImplementedException();
            }

            public int IndexOf(ArraySegment<byte> item)
            {
                // look forward
                Node current = this.root;
                int index = idx;
                while (current != null)
                {
                    if (Equals(current.segment, item))
                        return index;
                    current = current.next;
                    index++;
                }
                // look backwards
                current = this.root;
                index = idx;
                while (current != null)
                {
                    if (Equals(current.segment, item))
                        return index;
                    current = current.prev;
                    index--;
                }
                return -1;
            }

            public void Insert(int index, ArraySegment<byte> item)
            {
                throw new NotImplementedException();
            }

            public void Add(ArraySegment<byte> item)
            {
                if (this.root == null)
                {
                    this.root = new Node { segment = item };
                    this.count = 1;
                    this.idx = 0;
                }

                // find end
                while (this.root.next != null)
                {
                    this.root = root.next;
                    this.idx++;
                }

                this.root.next = new Node { segment = item, prev = this.root };
                this.count++;
                this.idx++;
            }

            public bool Contains(ArraySegment<byte> item)
            {
                return IndexOf(item) != -1;
            }

            public void CopyTo(ArraySegment<byte>[] array, int arrayIndex)
            {
                for (int i = arrayIndex; i < count; i++)
                {
                    array.SetValue(this[i], i);
                }
            }

            public bool Remove(ArraySegment<byte> item)
            {
                throw new NotImplementedException();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }
        }
    }
}