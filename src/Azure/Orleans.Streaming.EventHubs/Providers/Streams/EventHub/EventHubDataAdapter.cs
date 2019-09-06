using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Azure.EventHubs;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.ServiceBus.Providers
{
    /// <summary>
    /// Default event hub data adapter.  Users may subclass to override event data to stream mapping.
    /// </summary>
    public class EventHubDataAdapter : IEventHubDataAdapter
    {
        private readonly SerializationManager serializationManager;

        /// <summary>
        /// Cache data adapter that adapts EventHub's EventData to CachedEventHubMessage used in cache
        /// </summary>
        /// <param name="serializationManager"></param>
        public EventHubDataAdapter(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        /// <summary>
        /// Converts a cached message to a batch container for delivery
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        public virtual IBatchContainer GetBatchContainer(in CachedMessage cachedMessage)
        {
            var evenHubMessage = new EventHubMessage(cachedMessage, this.serializationManager);
            return GetBatchContainer(evenHubMessage);
        }

        /// <summary>
        /// Convert an EventHubMessage to a batch container
        /// </summary>
        /// <param name="eventHubMessage"></param>
        /// <returns></returns>
        protected virtual IBatchContainer GetBatchContainer(EventHubMessage eventHubMessage)
        {
            return new EventHubBatchContainer(eventHubMessage, this.serializationManager);
        }

        public virtual EventData ToQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("EventHub streams currently does not support non-null StreamSequenceToken.", nameof(token));
            return EventHubBatchContainer.ToEventData(this.serializationManager, streamGuid, streamNamespace, events, requestContext);
        }

        public virtual CachedMessage FromQueueMessage(StreamPosition streamPosition, EventData queueMessage, in DateTime dequeueTime, Func<int, ArraySegment<byte>> getSegment)
        {
            var cachedMessage =  new CachedMessage()
            {
                EnqueueTimeUtc = queueMessage.SystemProperties.EnqueuedTimeUtc,
                DequeueTimeUtc = dequeueTime,
            };

            EncodeMessageIntoSegment(
                streamPosition,
                queueMessage,
                getSegment,
                out cachedMessage.Segment,
                out cachedMessage.SequenceToken,
                out cachedMessage.StreamId,
                out cachedMessage.Payload);

            return cachedMessage;
        }

        public virtual StreamPosition GetStreamPosition(string partition, EventData queueMessage)
        {
            Guid streamGuid = Guid.Parse(queueMessage.SystemProperties.PartitionKey);
            string streamNamespace = queueMessage.GetStreamNamespaceProperty();
            var stremIdentity = new StreamIdentityToken(streamGuid, streamNamespace);
            ArraySegment<byte> token = queueMessage.GetSequenceToken();
            return new StreamPosition(stremIdentity.Token, token);
        }

        /// <summary>
        /// Get offset from cached message.  Left to derived class, as only it knows how to get this from the cached message.
        /// </summary>
        public virtual string GetOffset(in CachedMessage lastItemPurged)
        {
            // TODO figure out how to get this from the adapter
            int readOffset = 0;
            return SegmentBuilder.ReadNextString(lastItemPurged.Segment, ref readOffset); // read offset
        }

        // Placed object message payload into a segment.
        protected virtual void EncodeMessageIntoSegment(
            StreamPosition streamPosition,
            EventData queueMessage,
            Func<int, ArraySegment<byte>> getSegment,
            out ArraySegment<byte> segment,
            out (int Offset, int Count) sequenceToken,
            out (int Offset, int Count) streamIdToken,
            out (int Offset, int Count) payload)
        {
            byte[] propertiesBytes = queueMessage.SerializeProperties(this.serializationManager);
            byte[] body = queueMessage.Body.Array;
            // get size of namespace, offset, partitionkey, properties, and payload
            int size =
                SegmentBuilder.CalculateAppendSize(streamPosition.SequenceToken) +
                SegmentBuilder.CalculateAppendSize(streamPosition.StreamIdentity) +
                SegmentBuilder.CalculateAppendSize(queueMessage.SystemProperties.Offset) +
                SegmentBuilder.CalculateAppendSize(queueMessage.SystemProperties.PartitionKey) +
                SegmentBuilder.CalculateAppendSize(propertiesBytes) +
                SegmentBuilder.CalculateAppendSize(body);

            // get segment
            segment = getSegment(size);

            // encode sequence token
            int writeOffset = sequenceToken.Offset = 0;
            SegmentBuilder.Append(segment, ref writeOffset, streamPosition.SequenceToken);
            sequenceToken.Count = writeOffset - sequenceToken.Offset;

            // encode streamId
            streamIdToken.Offset = writeOffset;
            SegmentBuilder.Append(segment, ref writeOffset, streamPosition.StreamIdentity);
            streamIdToken.Count = writeOffset - streamIdToken.Offset;

            // encode payload
            payload.Offset = writeOffset;
            SegmentBuilder.Append(segment, ref writeOffset, queueMessage.SystemProperties.Offset);
            SegmentBuilder.Append(segment, ref writeOffset, queueMessage.SystemProperties.PartitionKey);
            SegmentBuilder.Append(segment, ref writeOffset, propertiesBytes);
            SegmentBuilder.Append(segment, ref writeOffset, body);
            payload.Count = writeOffset - payload.Offset;
        }
    }
}
