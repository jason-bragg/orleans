using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Azure.EventHubs;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System.Collections.Concurrent;
using Orleans.Runtime;

namespace Orleans.ServiceBus.Providers
{
    /// <summary>
    /// This is a tightly packed cached structure containing an event hub message.
    /// It should only contain value types.
    /// </summary>
    public struct CachedEventHubMessage
    {
        /// <summary>
        /// Guid of streamId this event is part of
        /// </summary>
        public Guid StreamGuid;
        /// <summary>
        /// EventHub sequence number.  Position of event in partition
        /// </summary>
        public long SequenceNumber;
        /// <summary>
        /// Time event was written to EventHub
        /// </summary>
        public DateTime EnqueueTimeUtc;
        /// <summary>
        /// Time event was read from EventHub into this cache
        /// </summary>
        public DateTime DequeueTimeUtc;
        /// <summary>
        /// Segment containing the serialized event data
        /// </summary>
        public ArraySegment<byte> Segment;
    }

    /// <summary>
    /// Replication of EventHub EventData class, reconstructed from cached data CachedEventHubMessage
    /// </summary>
    [Serializable]
    public class EventHubMessage
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="streamIdentity">Stream Identity</param>
        /// <param name="partitionKey">EventHub partition key for message</param>
        /// <param name="offset">Offset into the EventHub partition where this message was from</param>
        /// <param name="sequenceNumber">Offset into the EventHub partition where this message was from</param>
        /// <param name="enqueueTimeUtc">Time in UTC when this message was injected by EventHub</param>
        /// <param name="dequeueTimeUtc">Time in UTC when this message was read from EventHub into the current service</param>
        /// <param name="properties">User properties from EventData object</param>
        /// <param name="payload">Binary data from EventData object</param>
        public EventHubMessage(IStreamIdentity streamIdentity, string partitionKey, string offset, long sequenceNumber,
            DateTime enqueueTimeUtc, DateTime dequeueTimeUtc, IDictionary<string, object> properties, byte[] payload)
        {
            StreamIdentity = streamIdentity;
            PartitionKey = partitionKey;
            Offset = offset;
            SequenceNumber = sequenceNumber;
            EnqueueTimeUtc = enqueueTimeUtc;
            DequeueTimeUtc = dequeueTimeUtc;
            Properties = properties;
            Payload = payload;
        }

        /// <summary>
        /// Duplicate of EventHub's EventData class.
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <param name="serializationManager"></param>
        public EventHubMessage(CachedEventHubMessage cachedMessage, SerializationManager serializationManager)
        {
            int readOffset = 0;
            StreamIdentity = new StreamIdentity(cachedMessage.StreamGuid, SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset));
            Offset = SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset);
            PartitionKey = SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset);
            SequenceNumber = cachedMessage.SequenceNumber;
            EnqueueTimeUtc = cachedMessage.EnqueueTimeUtc;
            DequeueTimeUtc = cachedMessage.DequeueTimeUtc;
            Properties = SegmentBuilder.ReadNextBytes(cachedMessage.Segment, ref readOffset).DeserializeProperties(serializationManager);
            Payload = SegmentBuilder.ReadNextBytes(cachedMessage.Segment, ref readOffset).ToArray();
        }

        /// <summary>
        /// Stream identifier
        /// </summary>
        public IStreamIdentity StreamIdentity { get; }
        /// <summary>
        /// EventHub partition key
        /// </summary>
        public string PartitionKey { get; }
        /// <summary>
        /// Offset into EventHub partition
        /// </summary>
        public string Offset { get; }
        /// <summary>
        /// Sequence number in EventHub partition
        /// </summary>
        public long SequenceNumber { get; }
        /// <summary>
        /// Time event was written to EventHub
        /// </summary>
        public DateTime EnqueueTimeUtc { get; }
        /// <summary>
        /// Time event was read from EventHub and added to cache
        /// </summary>
        public DateTime DequeueTimeUtc { get; }
        /// <summary>
        /// User EventData properties
        /// </summary>
        public IDictionary<string, object> Properties { get; }
        /// <summary>
        /// Binary event data
        /// </summary>
        public byte[] Payload { get; }
    }

    /// <summary>
    /// Default eventhub data comparer.  Implements comparisons against CachedEventHubMessage
    /// </summary>
    public class EventHubDataComparer : ICacheDataComparer<CachedEventHubMessage>
    {
        /// <summary>
        /// Singleton instance, since type is stateless using this will reduce allocations.
        /// </summary>
        public static readonly ICacheDataComparer<CachedEventHubMessage> Instance = new EventHubDataComparer();

        /// <summary>
        /// Compare a cached message with a sequence token to determine if it message is before or after the token
        /// </summary>
        public int Compare(CachedEventHubMessage cachedMessage, StreamSequenceToken streamToken)
        {
            var realToken = (EventSequenceToken)streamToken;
            return (int)(cachedMessage.SequenceNumber - realToken.SequenceNumber);
        }

        /// <summary>
        /// Checks to see if the cached message is part of the provided stream
        /// </summary>
        public bool Equals(CachedEventHubMessage cachedMessage, IStreamIdentity streamIdentity)
        {
            int result = cachedMessage.StreamGuid.CompareTo(streamIdentity.Guid);
            if (result != 0) return false;

            int readOffset = 0;
            string decodedStreamNamespace = SegmentBuilder.ReadNextString(cachedMessage.Segment, ref readOffset);
            return string.Compare(decodedStreamNamespace, streamIdentity.Namespace, StringComparison.Ordinal) == 0;
        }
    }

    /// <summary>
    /// Default event hub data adapter.  Users may subclass to override event data to stream mapping.
    /// </summary>
    public class EventHubDataAdapter : ICacheDataAdapter<CachedEventHubMessage>
    {
        private readonly SerializationManager serializationManager;

        /// <inheritdoc />
        public Action<FixedSizeBuffer> OnBlockAllocated { set; private get; }

        /// <summary>
        /// Cache data adapter that adapts EventHub's EventData to CachedEventHubMessage used in cache
        /// </summary>
        /// <param name="serializationManager"></param>
        public EventHubDataAdapter(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        /// <inheritdoc />
        public DateTime? GetMessageEnqueueTimeUtc(ref CachedEventHubMessage message)
        {
            return message.EnqueueTimeUtc;
        }

        /// <inheritdoc />
        public DateTime? GetMessageDequeueTimeUtc(ref CachedEventHubMessage message)
        {
            return message.DequeueTimeUtc;
        }

        /// <summary>
        /// Converts a cached message to a batch container for delivery
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        public IBatchContainer GetBatchContainer(ref CachedEventHubMessage cachedMessage)
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

        /// <summary>
        /// Gets the stream sequence token from a cached message.
        /// </summary>
        /// <param name="cachedMessage"></param>
        /// <returns></returns>
        public virtual StreamSequenceToken GetSequenceToken(ref CachedEventHubMessage cachedMessage)
        {
            return new EventHubSequenceTokenV2("", cachedMessage.SequenceNumber, 0);
        }
    }
}
