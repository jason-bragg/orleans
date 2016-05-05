
using System;
using System.Collections.Generic;
using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Common.Utilities;
using DotNetty.Transport.Channels;
using Orleans.Runtime;

namespace Orleans.Messaging.Protocol
{
    public class MessageFrameDecoder : ByteToMessageDecoder
    {
        protected override void Decode(IChannelHandlerContext context, IByteBuffer input, List<object> output)
        {
            int headerLength;
            int bodyLength;
            if (!this.TryGetLengths(input, out headerLength, out bodyLength))
            {
                return;
            }
            int actualFrameLength = MessageFrameConstants.LenghtsSize + headerLength + bodyLength;
            if (input.ReadableBytes < actualFrameLength)
            {
                return;
            }
            byte[] headerBuffer = input.Array.Slice(input.ReaderIndex + MessageFrameConstants.LenghtsSize, headerLength);
            byte[] bodyBuffer = input.Array.Slice(input.ReaderIndex + MessageFrameConstants.LenghtsSize + headerLength, bodyLength);
            input.SetReaderIndex(input.ReaderIndex + actualFrameLength);
            var message = new Message(new List<ArraySegment<byte>>
            {
                new ArraySegment<byte>(headerBuffer, 0, headerBuffer.Length)
            }, new List<ArraySegment<byte>>
            {
                new ArraySegment<byte>(bodyBuffer, 0, bodyBuffer.Length)
            });
            output.Add(message);
        }

        private bool TryGetLengths(IByteBuffer input, out int headerLength, out int bodyLength)
        {
            headerLength = 0;
            bodyLength = 0;
            if (input.ReadableBytes < MessageFrameConstants.LenghtsSize)
                return false;
            headerLength = input.GetInt(input.ReaderIndex);
            bodyLength = input.GetInt(input.ReaderIndex+sizeof(int));
            return true;
        }
    }
}}
