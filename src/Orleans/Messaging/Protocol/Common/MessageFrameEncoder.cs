
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
        MessageFrameEncoder
        protected override void Encode(IChannelHandlerContext context, Message message, List<object> output)
        {
            List<ArraySegment<byte>> data;
            int headerLength = 0;
            try
            {
                data = message.Serialize(out headerLength);
            }
            catch (Exception ex)
            {
                throw new OrleansSerializationException()
            }
            IByteBuffer buffer = context.Allocator.Buffer(data.Sum(seg => seg.Count));
            foreach (ArraySegment<byte> segment in data)
            {
                buffer.WriteBytes(segment.ToArray());
            }
            output.Add(buffer.Retain());
        }
    }
}