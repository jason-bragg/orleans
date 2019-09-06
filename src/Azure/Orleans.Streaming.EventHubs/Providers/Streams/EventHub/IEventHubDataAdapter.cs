using System;
using Microsoft.Azure.EventHubs;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Orleans.ServiceBus.Providers
{
    public interface IEventHubDataAdapter : IQueueDataAdapter<EventData>, ICacheDataAdapter
    {
        /// <summary>
        /// Pack EventData into cached message
        /// </summary>
        CachedMessage Pack(StreamPosition streamPosition, EventData queueMessage, in DateTime dequeueTime, Func<int, ArraySegment<byte>> getSegment);

        StreamPosition GetStreamPosition(string partition, EventData queueMessage);

        string GetOffset(in CachedMessage cachedMessage);
    }
}
