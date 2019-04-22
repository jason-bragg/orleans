using System;
using System.Collections.Generic;
using Microsoft.Azure.EventHubs;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.ServiceBus.Providers
{
    public class EventHubQueueDataAdapter : IQueueDataAdapter<EventData, CachedEventHubMessage>
    {
        SerializationManager serializationManager;

        public EventHubQueueDataAdapter(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager ?? throw new ArgumentNullException(nameof(serializationManager));
        }

        public CachedEventHubMessage FromQueueMessage(EventData queueMessage, long sequenceId)
        {
            StreamPosition streamPosition = GetStreamPosition(queueMessage);
            return new CachedEventHubMessage()
            {
                StreamGuid = streamPosition.StreamIdentity.Guid,
                SequenceNumber = queueMessage.SystemProperties.SequenceNumber,
                EnqueueTimeUtc = queueMessage.SystemProperties.EnqueuedTimeUtc,
                Segment = EncodeMessageIntoSegment(streamPosition, queueMessage)
            };
        }

        public EventData ToQueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            return EventHubBatchContainer.ToEventData(this.serializationManager, streamGuid, streamNamespace, events, requestContext);
        }

        // Placed object message payload into a segment.
        protected virtual ArraySegment<byte> EncodeMessageIntoSegment(StreamPosition streamPosition, EventData queueMessage)
        {
            byte[] propertiesBytes = queueMessage.SerializeProperties(this.serializationManager);
            byte[] payload = queueMessage.Body.Array;
            // get size of namespace, offset, partitionkey, properties, and payload
            int size = SegmentBuilder.CalculateAppendSize(streamPosition.StreamIdentity.Namespace) +
            SegmentBuilder.CalculateAppendSize(queueMessage.SystemProperties.Offset) +
            SegmentBuilder.CalculateAppendSize(queueMessage.SystemProperties.PartitionKey) +
            SegmentBuilder.CalculateAppendSize(propertiesBytes) +
            SegmentBuilder.CalculateAppendSize(payload);

            // get segment
            ArraySegment<byte> segment = new ArraySegment<byte>(new byte[size]);

            // encode namespace, offset, partitionkey, properties and payload into segment
            int writeOffset = 0;
            SegmentBuilder.Append(segment, ref writeOffset, streamPosition.StreamIdentity.Namespace);
            SegmentBuilder.Append(segment, ref writeOffset, queueMessage.SystemProperties.Offset);
            SegmentBuilder.Append(segment, ref writeOffset, queueMessage.SystemProperties.PartitionKey);
            SegmentBuilder.Append(segment, ref writeOffset, propertiesBytes);
            SegmentBuilder.Append(segment, ref writeOffset, payload);

            return segment;
        }

        /// <summary>
        /// Gets the stream position from a queue message
        /// </summary>
        /// <param name="queueMessage"></param>
        /// <returns></returns>
        protected virtual StreamPosition GetStreamPosition(EventData queueMessage)
        {
            Guid streamGuid =
            Guid.Parse(queueMessage.SystemProperties.PartitionKey);
            string streamNamespace = queueMessage.GetStreamNamespaceProperty();
            IStreamIdentity stremIdentity = new StreamIdentity(streamGuid, streamNamespace);
            StreamSequenceToken token =
                new EventHubSequenceTokenV2(queueMessage.SystemProperties.Offset, queueMessage.SystemProperties.SequenceNumber, 0);
            return new StreamPosition(stremIdentity, token);
        }
    }
}
