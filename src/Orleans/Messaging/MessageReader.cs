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

namespace Orleans.Runtime
{
    internal class MessageReader
    {
        public List<ArraySegment<byte>> RecieveBuffer
        {
            get { return ByteArrayBuilder.BuildSegmentList(readBuffer, readOffset); }
        }

        private const int DefaultReadBufferSize = 1 << 17; // 128k
        private readonly List<ArraySegment<byte>> readBuffer;

        private readonly byte[] lengthBuffer;

        private int headerLength;
        private int bodyLength;

        private int readOffset;
        private int parseOffset;

        public MessageReader(int readBufferSize = DefaultReadBufferSize)
        {
            lengthBuffer = new byte[Message.LENGTH_HEADER_SIZE];
            readBuffer = BufferPool.GlobalPool.GetMultiBuffer(readBufferSize);
        }

        public void UpdateDataRead(int bytesRead)
        {
            readOffset += bytesRead;
        }


        public bool TryReadMessage(out Message msg)
        {
            msg = null;

            if (readOffset < headerLength + bodyLength + Message.LENGTH_HEADER_SIZE + parseOffset)
                return false;

            // read lengths if needed
            if (headerLength == 0 || bodyLength == 0)
            {
                // get length segments
                List<ArraySegment<byte>> lenghts = ByteArrayBuilder.GetSubSegments(readBuffer, parseOffset, Message.LENGTH_HEADER_SIZE);

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

            // have we read full message
            if (readOffset < headerLength + bodyLength + Message.LENGTH_HEADER_SIZE + parseOffset)
                return false;

            // read header
            int headerOffset = parseOffset + Message.LENGTH_HEADER_SIZE;
            List<ArraySegment<byte>> header = ByteArrayBuilder.GetSubSegments(readBuffer, headerOffset, headerLength);
            if (header.Sum(s => s.Count) != headerLength)
            {
                throw new ApplicationException("header size off");
            }

            // read body
            int bodyOffset = headerOffset + headerLength;
            List<ArraySegment<byte>> body = ByteArrayBuilder.GetSubSegments(readBuffer, bodyOffset, bodyLength);
            if (body.Sum(s => s.Count) != bodyLength)
            {
                throw new ApplicationException("body size off");
            }

            msg = new Message(header, body);
            MessagingStatisticsGroup.OnMessageReceive(msg, headerLength, bodyLength);
            
            // update parse readOffset and clear lengths
            parseOffset = bodyOffset + bodyLength;
            headerLength = 0;
            bodyLength = 0;

            // drop buffers consumed in message and adjust parse readOffset
            int consumedBytes = 0;
            while (readBuffer.Count != 0)
            {
                ArraySegment<byte> seg = readBuffer.First();
                if (seg.Count <= parseOffset - consumedBytes)
                {
                    consumedBytes += seg.Count;
                    readBuffer.Remove(seg);
                    BufferPool.GlobalPool.Release(seg.Array);
                }
                break;
            }
            parseOffset -= consumedBytes;
            readOffset -= consumedBytes;
            if (parseOffset > readOffset)
            {
                throw new ApplicationException("parse ahead of read, not possible");
            }

            // back fill buffer
            if (consumedBytes != 0)
                readBuffer.AddRange(BufferPool.GlobalPool.GetMultiBuffer(consumedBytes));

            return true;
        }

        public void Reset()
        {
            readOffset = 0;
            parseOffset = 0;
            headerLength = 0;
            bodyLength = 0;
        }
    }
}
