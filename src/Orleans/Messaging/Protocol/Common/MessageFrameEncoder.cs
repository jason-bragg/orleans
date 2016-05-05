
using System;
using System.Collections.Generic;
using System.Linq;
using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using Orleans.Runtime;

namespace Orleans.Messaging.Protocol
{
    internal class MessageFrameEncoder : MessageToMessageEncoder<Message>
    {
        protected override void Encode(IChannelHandlerContext context, Message message, List<object> output)
        {
            int headerLength;
            var data = message.Serialize(out headerLength);
            IByteBuffer buffer = context.Allocator.Buffer(data.Sum(seg => seg.Count));
            foreach (ArraySegment<byte> segment in data)
            {
                buffer.WriteBytes(segment.ToArray());
            }
            output.Add(buffer.Retain());
        }
    }
}