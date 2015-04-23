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

﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans.Runtime.Configuration;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    public enum MsgHeader : byte
    {
        ALWAYS_INTERLEAVE = 1,
        CACHE_INVALIDATION_HEADER,
        CATEGORY,
        CORRELATION_ID,
        DEBUG_CONTEXT,
        DIRECTION,
        EXPIRATION,
        FORWARD_COUNT,
        INTERFACE_ID,
        METHOD_ID,
        NEW_GRAIN_TYPE,
        GENERIC_GRAIN_TYPE,
        RESULT,
        REJECTION_INFO,
        REJECTION_TYPE,
        READ_ONLY,
        RESEND_COUNT,
        SENDING_ACTIVATION,
        SENDING_GRAIN,
        SENDING_SILO,
        IS_NEW_PLACEMENT,

        TARGET_ACTIVATION,
        TARGET_GRAIN,
        TARGET_SILO,
        TARGET_OBSERVER,
        TIMESTAMPS,
        IS_UNORDERED,

        PING_APPLICATION_HEADER,
        PRIOR_MESSAGE_ID,
        PRIOR_MESSAGE_TIMES,

        CALL_CHAIN_REQUEST_CONTEXT_HEADER,
        E2_E_TRACING_ACTIVITY_ID_HEADER,
        
        // must always be last
        APPLICATION_HEADER_FLAG,
    }

    [Serializable]
    internal class Message : IOutgoingMessage
    {

        public static class Metadata
        {
            public const string MAX_RETRIES = "MaxRetries";
            public const string EXCLUDE_TARGET_ACTIVATIONS = "#XA";
            public const string TARGET_HISTORY = "TargetHistory";
            public const string ACTIVATION_DATA = "ActivationData";
        }


        public static int LargeMessageSizeThreshold { get; set; }
        public const int LENGTH_HEADER_SIZE = 8;
        public const int LENGTH_META_HEADER = 4;

        private readonly Dictionary<byte, object> headers;
        [NonSerialized]
        private Dictionary<string, object> metadata;

        /// <summary>
        /// NOTE: The contents of bodyBytes should never be modified
        /// </summary>
        private List<ArraySegment<byte>> bodyBytes;

        private List<ArraySegment<byte>> headerBytes;

        private object bodyObject;

        // Cache values of TargetAddess and SendingAddress as they are used very frequently
        private ActivationAddress targetAddress;
        private ActivationAddress sendingAddress;
        private static readonly TraceLogger logger;
        private static readonly Dictionary<string, TransitionStats[,]> lifecycleStatistics;

        internal static bool WriteMessagingTraces { get; set; }
        
        static Message()
        {
            lifecycleStatistics = new Dictionary<string, TransitionStats[,]>();
            logger = TraceLogger.GetLogger("Message", TraceLogger.LoggerType.Runtime);
        }

        public enum Categories
        {
            Ping = 1,
            System,
            Application,
        }

        public enum Directions
        {
            Request = 1,
            Response,
            OneWay
        }

        public enum ResponseTypes
        {
            Success = 1,
            Error,
            Rejection
        }

        public enum RejectionTypes
        {
            Transient = 1,
            Overloaded,
            DuplicateRequest,
            Unrecoverable,
            GatewayTooBusy,
        }

        public Categories Category
        {
            get { return GetScalarHeader<Categories>(MsgHeader.CATEGORY); }
            set { SetHeader(MsgHeader.CATEGORY, value); }
        }

        public Directions Direction
        {
            get { return GetScalarHeader<Directions>(MsgHeader.DIRECTION); }
            set { SetHeader(MsgHeader.DIRECTION, value); }
        }

        public bool IsReadOnly
        {
            get { return GetScalarHeader<bool>(MsgHeader.READ_ONLY); }
            set { SetHeader(MsgHeader.READ_ONLY, value); }
        }

        public bool IsAlwaysInterleave
        {
            get { return GetScalarHeader<bool>(MsgHeader.ALWAYS_INTERLEAVE); }
            set { SetHeader(MsgHeader.ALWAYS_INTERLEAVE, value); }
        }

        public bool IsUnordered
        {
            get { return GetScalarHeader<bool>(MsgHeader.IS_UNORDERED); }
            set
            {
                if (value || ContainsHeader(MsgHeader.IS_UNORDERED))
                    SetHeader(MsgHeader.IS_UNORDERED, value);
            }
        }

        public CorrelationId Id
        {
            get { return GetSimpleHeader<CorrelationId>(MsgHeader.CORRELATION_ID); }
            set { SetHeader(MsgHeader.CORRELATION_ID, value); }
        }

        public int ResendCount
        {
            get { return GetScalarHeader<int>(MsgHeader.RESEND_COUNT); }
            set { SetHeader(MsgHeader.RESEND_COUNT, value); }
        }

        public int ForwardCount
        {
            get { return GetScalarHeader<int>(MsgHeader.FORWARD_COUNT); }
            set { SetHeader(MsgHeader.FORWARD_COUNT, value); }
        }

        public SiloAddress TargetSilo
        {
            get { return (SiloAddress)GetHeader(MsgHeader.TARGET_SILO); }
            set
            {
                SetHeader(MsgHeader.TARGET_SILO, value);
                targetAddress = null;
            }
        }

        public GrainId TargetGrain
        {
            get { return GetSimpleHeader<GrainId>(MsgHeader.TARGET_GRAIN); }
            set
            {
                SetHeader(MsgHeader.TARGET_GRAIN, value);
                targetAddress = null;
            }
        }

        public ActivationId TargetActivation
        {
            get { return GetSimpleHeader<ActivationId>(MsgHeader.TARGET_ACTIVATION); }
            set
            {
                SetHeader(MsgHeader.TARGET_ACTIVATION, value);
                targetAddress = null;
            }
        }

        public ActivationAddress TargetAddress
        {
            get { return targetAddress ?? (targetAddress = ActivationAddress.GetAddress(TargetSilo, TargetGrain, TargetActivation)); }
            set
            {
                TargetGrain = value.Grain;
                TargetActivation = value.Activation;
                TargetSilo = value.Silo;
                targetAddress = value;
            }
        }

        public GuidId TargetObserverId
        {
            get { return GetSimpleHeader<GuidId>(MsgHeader.TARGET_OBSERVER); }
            set
            {
                SetHeader(MsgHeader.TARGET_OBSERVER, value);
                targetAddress = null;
            }
        }

        public SiloAddress SendingSilo
        {
            get { return (SiloAddress)GetHeader(MsgHeader.SENDING_SILO); }
            set
            {
                SetHeader(MsgHeader.SENDING_SILO, value);
                sendingAddress = null;
            }
        }

        public GrainId SendingGrain
        {
            get { return GetSimpleHeader<GrainId>(MsgHeader.SENDING_GRAIN); }
            set
            {
                SetHeader(MsgHeader.SENDING_GRAIN, value);
                sendingAddress = null;
            }
        }

        public ActivationId SendingActivation
        {
            get { return GetSimpleHeader<ActivationId>(MsgHeader.SENDING_ACTIVATION); }
            set
            {
                SetHeader(MsgHeader.SENDING_ACTIVATION, value);
                sendingAddress = null;
            }
        }

        public ActivationAddress SendingAddress
        {
            get { return sendingAddress ?? (sendingAddress = ActivationAddress.GetAddress(SendingSilo, SendingGrain, SendingActivation)); }
            set
            {
                SendingGrain = value.Grain;
                SendingActivation = value.Activation;
                SendingSilo = value.Silo;
                sendingAddress = value;
            }
        }

        public bool IsNewPlacement
        {
            get { return GetScalarHeader<bool>(MsgHeader.IS_NEW_PLACEMENT); }
            set
            {
                if (value || ContainsHeader(MsgHeader.IS_NEW_PLACEMENT))
                    SetHeader(MsgHeader.IS_NEW_PLACEMENT, value);
            }
        }

        public ResponseTypes Result
        {
            get { return GetScalarHeader<ResponseTypes>(MsgHeader.RESULT); }
            set { SetHeader(MsgHeader.RESULT, value); }
        }

        public DateTime Expiration
        {
            get { return GetScalarHeader<DateTime>(MsgHeader.EXPIRATION); }
            set { SetHeader(MsgHeader.EXPIRATION, value); }
        }

        public bool IsExpired
        {
            get { return (ContainsHeader(MsgHeader.EXPIRATION)) && DateTime.UtcNow > Expiration; }
        }

        public bool IsExpirableMessage(IMessagingConfiguration config)
        {
            if (!config.DropExpiredMessages) return false;

            GrainId id = TargetGrain;
            if (id == null) return false;

            // don't set expiration for one way, system target and system grain messages.
            return Direction != Directions.OneWay && !id.IsSystemTarget && !Constants.IsSystemGrain(id);
        }
        
        public string DebugContext
        {
            get { return GetStringHeader(MsgHeader.DEBUG_CONTEXT); }
            set { SetHeader(MsgHeader.DEBUG_CONTEXT, value); }
        }

        public IEnumerable<ActivationAddress> CacheInvalidationHeader
        {
            get
            {
                object obj = GetHeader(MsgHeader.CACHE_INVALIDATION_HEADER);
                return obj == null ? null : ((IEnumerable)obj).Cast<ActivationAddress>();
            }
        }

        internal void AddToCacheInvalidationHeader(ActivationAddress address)
        {
            var list = new List<ActivationAddress>();
            if (ContainsHeader(MsgHeader.CACHE_INVALIDATION_HEADER))
            {
                var prevList = ((IEnumerable)GetHeader(MsgHeader.CACHE_INVALIDATION_HEADER)).Cast<ActivationAddress>();
                list.AddRange(prevList);
            }
            list.Add(address);
            SetHeader(MsgHeader.CACHE_INVALIDATION_HEADER, list);
        }

        // Resends are used by the sender, usualy due to en error to send or due to a transient rejection.
        public bool MayResend(IMessagingConfiguration config)
        {
            return ResendCount < config.MaxResendCount;
        }

        // Forwardings are used by the receiver, usualy when it cannot process the message and forwars it to another silo to perform the processing
        // (got here due to outdated cache, silo is shutting down/overloaded, ...).
        public bool MayForward(GlobalConfiguration config)
        {
            return ForwardCount < config.MaxForwardCount;
        }

        public int MethodId
        {
            get { return GetScalarHeader<int>(MsgHeader.METHOD_ID); }
            set { SetHeader(MsgHeader.METHOD_ID, value); }
        }

        public int InterfaceId
        {
            get { return GetScalarHeader<int>(MsgHeader.INTERFACE_ID); }
            set { SetHeader(MsgHeader.INTERFACE_ID, value); }
        }

        /// <summary>
        /// Set by sender's placement logic when NewPlacementRequested is true
        /// so that receiver knows desired grain type
        /// </summary>
        public string NewGrainType
        {
            get { return GetStringHeader(MsgHeader.NEW_GRAIN_TYPE); }
            set { SetHeader(MsgHeader.NEW_GRAIN_TYPE, value); }
        }

        /// <summary>
        /// Set by caller's grain reference 
        /// </summary>
        public string GenericGrainType
        {
            get { return GetStringHeader(MsgHeader.GENERIC_GRAIN_TYPE); }
            set { SetHeader(MsgHeader.GENERIC_GRAIN_TYPE, value); }
        }

        public RejectionTypes RejectionType
        {
            get { return GetScalarHeader<RejectionTypes>(MsgHeader.REJECTION_TYPE); }
            set { SetHeader(MsgHeader.REJECTION_TYPE, value); }
        }

        public string RejectionInfo
        {
            get { return GetStringHeader(MsgHeader.REJECTION_INFO); }
            set { SetHeader(MsgHeader.REJECTION_INFO, value); }
        }


        public object BodyObject
        {
            get
            {
                if (bodyObject != null)
                {
                    return bodyObject;
                }
                if (bodyBytes == null)
                {
                    return null;
                }
                try
                {
                    var stream = new BinaryTokenStreamReader(bodyBytes);
                    bodyObject = SerializationManager.Deserialize(stream);
                }
                catch (Exception ex)
                {
                    logger.Error(ErrorCode.Messaging_UnableToDeserializeBody, "Exception deserializing message body", ex);
                    throw;
                }
                finally
                {
                    BufferPool.GlobalPool.Release(bodyBytes);
                    bodyBytes = null;
                }
                return bodyObject;
            }
            set
            {
                bodyObject = value;
                if (bodyBytes == null) return;

                BufferPool.GlobalPool.Release(bodyBytes);
                bodyBytes = null;
            }
        }

        public Message()
        {
            headers = new Dictionary<byte, object>();
            metadata = new Dictionary<string, object>();
            bodyObject = null;
            bodyBytes = null;
            headerBytes = null;
        }

        public Message(Categories type, Directions subtype)
            : this()
        {
            Category = type;
            Direction = subtype;
        }

        internal Message(byte[] header, byte[] body)
            : this(new List<ArraySegment<byte>> { new ArraySegment<byte>(header) },
                new List<ArraySegment<byte>> { new ArraySegment<byte>(body) })
        {
        }

        public Message(List<ArraySegment<byte>> header, List<ArraySegment<byte>> body)
        {
            metadata = new Dictionary<string, object>();

            var input = new BinaryTokenStreamReader(header);
            headers = SerializationManager.DeserializeMessageHeaders(input);
            BufferPool.GlobalPool.Release(header);

            bodyBytes = body;
            bodyObject = null;
            headerBytes = null;
        }

        public Message CreateResponseMessage()
        {
            var response = new Message(this.Category, Directions.Response)
            {
                Id = this.Id,
                IsReadOnly = this.IsReadOnly,
                IsAlwaysInterleave = this.IsAlwaysInterleave,
                TargetSilo = this.SendingSilo
            };

            if (this.ContainsHeader(MsgHeader.SENDING_GRAIN))
            {
                response.SetHeader(MsgHeader.TARGET_GRAIN, this.GetHeader(MsgHeader.SENDING_GRAIN));
                if (this.ContainsHeader(MsgHeader.SENDING_ACTIVATION))
                {
                    response.SetHeader(MsgHeader.TARGET_ACTIVATION, this.GetHeader(MsgHeader.SENDING_ACTIVATION));
                }
            }

            response.SendingSilo = this.TargetSilo;
            if (this.ContainsHeader(MsgHeader.TARGET_GRAIN))
            {
                response.SetHeader(MsgHeader.SENDING_GRAIN, this.GetHeader(MsgHeader.TARGET_GRAIN));
                if (this.ContainsHeader(MsgHeader.TARGET_ACTIVATION))
                {
                    response.SetHeader(MsgHeader.SENDING_ACTIVATION, this.GetHeader(MsgHeader.TARGET_ACTIVATION));
                }
                else if (this.TargetGrain.IsSystemTarget)
                {
                    response.SetHeader(MsgHeader.SENDING_ACTIVATION, ActivationId.GetSystemActivation(TargetGrain, TargetSilo));
                }
            }

            if (this.ContainsHeader(MsgHeader.TIMESTAMPS))
            {
                response.SetHeader(MsgHeader.TIMESTAMPS, this.GetHeader(MsgHeader.TIMESTAMPS));
            }
            if (this.ContainsHeader(MsgHeader.DEBUG_CONTEXT))
            {
                response.SetHeader(MsgHeader.DEBUG_CONTEXT, this.GetHeader(MsgHeader.DEBUG_CONTEXT));
            }
            if (this.ContainsHeader(MsgHeader.CACHE_INVALIDATION_HEADER))
            {
                response.SetHeader(MsgHeader.CACHE_INVALIDATION_HEADER, this.GetHeader(MsgHeader.CACHE_INVALIDATION_HEADER));
            }
            if (this.ContainsHeader(MsgHeader.EXPIRATION))
            {
                response.SetHeader(MsgHeader.EXPIRATION, this.GetHeader(MsgHeader.EXPIRATION));
            }
            if (Message.WriteMessagingTraces) response.AddTimestamp(LifecycleTag.CreateResponse);

            RequestContext.ExportToMessage(response);

            return response;
        }

        public Message CreateRejectionResponse(RejectionTypes type, string info)
        {
            var response = CreateResponseMessage();
            response.Result = ResponseTypes.Rejection;
            response.RejectionType = type;
            response.RejectionInfo = info;
            if (logger.IsVerbose) logger.Verbose("Creating {0} rejection with info '{1}' for {2} at:" + Environment.NewLine + "{3}", type, info, this, new System.Diagnostics.StackTrace(true));
            return response;
        }

        public bool ContainsHeader(MsgHeader tag)
        {
            return headers.ContainsKey((byte)tag);
        }

        public void RemoveHeader(MsgHeader tag)
        {
            lock (headers)
            {
                headers.Remove((byte)tag);
                if (tag == MsgHeader.TARGET_ACTIVATION || tag == MsgHeader.TARGET_GRAIN | tag == MsgHeader.TARGET_SILO)
                    targetAddress = null;
            }
        }

        public void SetHeader(MsgHeader tag, object value)
        {
            lock (headers)
            {
                headers[(byte)tag] = value;
            }
        }

        public object GetHeader(MsgHeader tag)
        {
            object val;
            bool flag;
            lock (headers)
            {
                flag = headers.TryGetValue((byte)tag, out val);
            }
            return flag ? val : null;
        }

        public string GetStringHeader(MsgHeader tag)
        {
            object val;
            if (!headers.TryGetValue((byte)tag, out val)) return String.Empty;

            var s = val as string;
            return s ?? String.Empty;
        }

        public T GetScalarHeader<T>(MsgHeader tag)
        {
            return GetScalarHeader<T>((byte)tag);
        }

        public T GetScalarHeader<T>(byte tag)
        {
            object val;
            if (headers.TryGetValue(tag, out val))
            {
                return (T)val;
            }
            return default(T);
        }

        public T GetSimpleHeader<T>(MsgHeader tag)
        {
            object val;
            if (!headers.TryGetValue((byte)tag, out val) || val == null) return default(T);

            return val is T ? (T) val : default(T);
        }

        internal void SetApplicationHeaders(Dictionary<byte, object> data)
        {
            lock (headers)
            {
                foreach (var item in data)
                {
                    byte key = (byte)(MsgHeader.APPLICATION_HEADER_FLAG + item.Key);
                    headers[key] = SerializationManager.DeepCopy(item.Value);
                }
            }
        }

        internal void GetApplicationHeaders(Dictionary<byte, object> dict)
        {
            TryGetApplicationHeaders(ref dict);
        }

        private void TryGetApplicationHeaders(ref Dictionary<byte, object> dict)
        {
            lock (headers)
            {
                foreach (var pair in headers)
                {
                    if (pair.Key > (byte)MsgHeader.APPLICATION_HEADER_FLAG) continue;

                    if (dict == null)
                    {
                        dict = new Dictionary<byte, object>();
                    }
                    dict[(byte)(pair.Key - MsgHeader.APPLICATION_HEADER_FLAG)] = pair.Value;
                }
            }
        }

        public object GetApplicationHeader(MsgHeader tag)
        {
            lock (headers)
            {
                object obj;
                return headers.TryGetValue((byte)(MsgHeader.APPLICATION_HEADER_FLAG + (byte)tag), out obj) ? obj : null;
            }
        }

        public bool ContainsMetadata(string tag)
        {
            return metadata != null && metadata.ContainsKey(tag);
        }

        public void SetMetadata(string tag, object data)
        {
            metadata = metadata ?? new Dictionary<string, object>();
            metadata[tag] = data;
        }

        public void RemoveMetadata(string tag)
        {
            if (metadata != null)
            {
                metadata.Remove(tag);
            }
        }

        public object GetMetadata(string tag)
        {
            object data;
            if (metadata != null && metadata.TryGetValue(tag, out data))
            {
                return data;
            }
            return null;
        }

        /// <summary>
        /// Tell whether two messages are duplicates of one another
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IsDuplicate(Message other)
        {
            return Equals(SendingSilo, other.SendingSilo) && Equals(Id, other.Id);
        }

        #region Message timestamping

        private class TransitionStats
        {
            ulong count;
            TimeSpan totalTime;
            TimeSpan maxTime;

            public TransitionStats()
            {
                count = 0;
                totalTime = TimeSpan.Zero;
                maxTime = TimeSpan.Zero;
            }

            public void RecordTransition(TimeSpan time)
            {
                lock (this)
                {
                    count++;
                    totalTime += time;
                    if (time > maxTime)
                    {
                        maxTime = time;
                    }
                }
            }

            public override string ToString()
            {
                var sb = new StringBuilder();

                if (count > 0)
                {
                    sb.AppendFormat("{0}\t{1}\t{2}", count, totalTime.Divide(count), maxTime);
                }

                return sb.ToString();
            }
        }

        public void AddTimestamp(LifecycleTag tag)
        {
            if (logger.IsVerbose2)
            {
                if (LogVerbose(tag))
                    logger.Verbose("Message {0} {1}", tag, this);
                else if (logger.IsVerbose2)
                    logger.Verbose2("Message {0} {1}", tag, this);
            }

            if (WriteMessagingTraces)
            {
                var now = DateTime.UtcNow;
                var timestamp = new List<object> {tag, now};

                object val;
                List<object> list = null;
                if (headers.TryGetValue((byte)MsgHeader.TIMESTAMPS, out val))
                {
                    list = val as List<object>;
                }
                if (list == null)
                {
                    list = new List<object>();
                    lock (headers)
                    {
                        headers[(byte)MsgHeader.TIMESTAMPS] = list;
                    }
                }
                else if (list.Count > 0)
                {
                    var last = list[list.Count - 1] as List<object>;
                    if (last != null)
                    {
                        var context = DebugContext;
                        if (String.IsNullOrEmpty(context))
                        {
                            context = "Unspecified";
                        }
                        TransitionStats[,] entry;
                        bool found;
                        lock (lifecycleStatistics)
                        {
                            found = lifecycleStatistics.TryGetValue(context, out entry);
                        }
                        if (!found)
                        {
                            var newEntry = new TransitionStats[32, 32];
                            for (int i = 0; i < 32; i++) for (int j = 0; j < 32; j++) newEntry[i, j] = new TransitionStats();
                            lock (lifecycleStatistics)
                            {
                                if (!lifecycleStatistics.TryGetValue(context, out entry))
                                {
                                    entry = newEntry;
                                    lifecycleStatistics.Add(context, entry);
                                }
                            }
                        }
                        int from = (int)(LifecycleTag)(last[0]);
                        int to = (int)tag;
                        entry[from, to].RecordTransition(now.Subtract((DateTime)last[1]));
                    }
                }
                list.Add(timestamp);
            }
            if (OnTrace != null)
                OnTrace(this, tag);
        }

        private static bool LogVerbose(LifecycleTag tag)
        {
            return tag == LifecycleTag.EnqueueOutgoing ||
                   tag == LifecycleTag.CreateNewPlacement ||
                   tag == LifecycleTag.EnqueueIncoming ||
                   tag == LifecycleTag.InvokeIncoming;
        }

        public List<Tuple<string, DateTime>> GetTimestamps()
        {
            var result = new List<Tuple<string, DateTime>>();

            object val;
            List<object> list = null;
            if (headers.TryGetValue((byte)MsgHeader.TIMESTAMPS, out val))
            {
                list = val as List<object>;
            }
            if (list == null) return result;

            foreach (object item in list)
            {
                var stamp = item as List<object>;
                if ((stamp != null) && (stamp.Count == 2) && (stamp[0] is string) && (stamp[1] is DateTime))
                {
                    result.Add(new Tuple<string, DateTime>(stamp[0] as string, (DateTime)stamp[1]));
                }
            }
            return result;
        }

        public string GetTimestampString(bool singleLine = true, bool includeTimes = true, int indent = 0)
        {
            var sb = new StringBuilder();

            object val;
            List<object> list = null;
            if (headers.TryGetValue((byte)MsgHeader.TIMESTAMPS, out val))
            {
                list = val as List<object>;
            }
            if (list == null) return sb.ToString();

            bool firstItem = true;
            var indentString = new string(' ', indent);
            foreach (object item in list)
            {
                var stamp = item as List<object>;
                if ((stamp == null) || (stamp.Count != 2) || (!(stamp[0] is string)) || (!(stamp[1] is DateTime)))
                    continue;

                if (!firstItem && singleLine)
                {
                    sb.Append(", ");
                }
                else if (!singleLine && (indent > 0))
                {
                    sb.Append(indentString);
                }
                sb.Append(stamp[0]);
                if (includeTimes)
                {
                    sb.Append(" ==> ");
                    var when = (DateTime)stamp[1];
                    sb.Append(when.ToString("HH:mm:ss.ffffff"));
                }
                if (!singleLine)
                {
                    sb.AppendLine();
                }
                firstItem = false;
            }
            return sb.ToString();
        }

        #endregion

        #region Serialization

        internal List<ArraySegment<byte>> Serialize()
        {
            int dummy1;
            int dummy2;
            return Serialize_Impl(false, out dummy1, out dummy2);
        }

        public List<ArraySegment<byte>> Serialize(out int headerLength)
        {
            int dummy;
            return Serialize_Impl(false, out headerLength, out dummy);
        }

        public List<ArraySegment<byte>> SerializeForBatching(out int headerLength, out int bodyLength)
        {
            return Serialize_Impl(true, out headerLength, out bodyLength);
        }

        private List<ArraySegment<byte>> Serialize_Impl(bool batching, out int headerLengthOut, out int bodyLengthOut)
        {
            var headerStream = new BinaryTokenStreamWriter();
            lock (headers) // Guard against any attempts to modify message headers while we are serializing them
            {
                SerializationManager.SerializeMessageHeaders(headers, headerStream);
            }

            if (bodyBytes == null)
            {
                var bodyStream = new BinaryTokenStreamWriter();
                SerializationManager.Serialize(bodyObject, bodyStream);
                // We don't bother to turn this into a byte array and save it in bodyBytes because Serialize only gets called on a message
                // being sent off-box. In this case, the likelihood of needed to re-serialize is very low, and the cost of capturing the
                // serialized bytes from the steam -- where they're a list of ArraySegment objects -- into an array of bytes is actually
                // pretty high (an array allocation plus a bunch of copying).
                bodyBytes = bodyStream.ToBytes() as List<ArraySegment<byte>>;
            }

            if (headerBytes != null)
            {
                BufferPool.GlobalPool.Release(headerBytes);
            }
            headerBytes = headerStream.ToBytes() as List<ArraySegment<byte>>;
            int headerLength = headerBytes.Sum(ab => ab.Count);
            int bodyLength = bodyBytes.Sum(ab => ab.Count);

            var bytes = new List<ArraySegment<byte>>();
            if (!batching)
            {
                bytes.Add(new ArraySegment<byte>(BitConverter.GetBytes(headerLength)));
                bytes.Add(new ArraySegment<byte>(BitConverter.GetBytes(bodyLength)));
            }
            bytes.AddRange(headerBytes);
            bytes.AddRange(bodyBytes);

            if (headerLength + bodyLength > LargeMessageSizeThreshold)
            {
                logger.Info(ErrorCode.Messaging_LargeMsg_Outgoing, "Preparing to send large message Size={0} HeaderLength={1} BodyLength={2} #ArraySegments={3}. Msg={4}",
                    headerLength + bodyLength + LENGTH_HEADER_SIZE, headerLength, bodyLength, bytes.Count, this.ToString());
                if (logger.IsVerbose3) logger.Verbose3("Sending large message {0}", this.ToLongString());
            }

            headerLengthOut = headerLength;
            bodyLengthOut = bodyLength;
            return bytes;
        }


        public void ReleaseBodyAndHeaderBuffers()
        {
            ReleaseHeadersOnly();
            ReleaseBodyOnly();
        }

        public void ReleaseHeadersOnly()
        {
            if (headerBytes == null) return;

            BufferPool.GlobalPool.Release(headerBytes);
            headerBytes = null;
        }

        public void ReleaseBodyOnly()
        {
            if (bodyBytes == null) return;

            BufferPool.GlobalPool.Release(bodyBytes);
            bodyBytes = null;
        }

        #endregion

        // For testing and logging/tracing
        public string ToLongString()
        {
            var sb = new StringBuilder();

            string debugContex = DebugContext;
            if (!string.IsNullOrEmpty(debugContex))
            {
                // if DebugContex is present, print it first.
                sb.Append(debugContex).Append(".");
            }

            lock (headers)
            {
                foreach (var pair in headers)
                {
                    if (pair.Key != (byte)MsgHeader.DEBUG_CONTEXT)
                    {
                        sb.AppendFormat("{0}={1};", pair.Key, pair.Value);
                    }
                    sb.AppendLine();
                }
            }
            return sb.ToString();
        }

        public override string ToString()
        {
            string response = String.Empty;
            if (Direction == Directions.Response)
            {
                switch (Result)
                {
                    case ResponseTypes.Error:
                        response = "Error ";
                        break;

                    case ResponseTypes.Rejection:
                        response = string.Format("{0} Rejection (info: {1}) ", RejectionType, RejectionInfo);
                        break;
                }
            }
            string times = this.GetStringHeader(MsgHeader.TIMESTAMPS);
            return String.Format("{0}{1}{2}{3}{4} {5}->{6} #{7}{8}{9}{10}: {11}",
                IsReadOnly ? "ReadOnly " : "", //0
                IsAlwaysInterleave ? "IsAlwaysInterleave " : "", //1
                IsNewPlacement ? "NewPlacement " : "", // 2
                response,  //3
                Direction, //4
                String.Format("{0}{1}{2}", SendingSilo, SendingGrain, SendingActivation), //5
                String.Format("{0}{1}{2}{3}", TargetSilo, TargetGrain, TargetActivation, TargetObserverId), //6
                Id, //7
                ResendCount > 0 ? "[ResendCount=" + ResendCount + "]" : "", //8
                ForwardCount > 0 ? "[ForwardCount=" + ForwardCount + "]" : "", //9
                string.IsNullOrEmpty(times) ? "" : "[" + times + "]", //10
                DebugContext); //11
        }

        /// <summary>
        /// Tags used to identify points in the message processing lifecycle for logging.
        /// Should be fewer than 32 since bit flags are used for filtering events.
        /// </summary>
        public enum LifecycleTag
        {
            Create = 0,
            EnqueueOutgoing,
            StartRouting,
            AsyncRouting,
            DoneRouting,
            SendOutgoing,
            ReceiveIncoming,
            RerouteIncoming,
            EnqueueForRerouting,
            EnqueueForForwarding,
            EnqueueIncoming,
            DequeueIncoming,
            CreateNewPlacement,
            TaskIncoming,
            TaskRedirect,
            EnqueueWaiting,
            EnqueueReady,
            EnqueueWorkItem,
            DequeueWorkItem,
            InvokeIncoming,
            CreateResponse,
        }

        /// <summary>
        /// Global function that is set to monitor message lifecycle events
        /// </summary>
        internal static Action<Message, LifecycleTag> OnTrace { private get; set; }

        internal void SetTargetPlacement(PlacementResult value)
        {
            if ((value.IsNewPlacement ||
                     (ContainsHeader(MsgHeader.TARGET_ACTIVATION) &&
                     !TargetActivation.Equals(value.Activation))))
            {
                RemoveHeader(MsgHeader.PRIOR_MESSAGE_ID);
                RemoveHeader(MsgHeader.PRIOR_MESSAGE_TIMES);
            }
            TargetActivation = value.Activation;
            TargetSilo = value.Silo;

            if (value.IsNewPlacement)
                IsNewPlacement = true;

            if (!String.IsNullOrEmpty(value.GrainType))
                NewGrainType = value.GrainType;
        }


        public string GetTargetHistory()
        {
            var history = new StringBuilder();
            history.Append("<");
            if (ContainsHeader(MsgHeader.TARGET_SILO))
            {
                history.Append(TargetSilo).Append(":");
            }
            if (ContainsHeader(MsgHeader.TARGET_GRAIN))
            {
                history.Append(TargetGrain).Append(":");
            }
            if (ContainsHeader(MsgHeader.TARGET_ACTIVATION))
            {
                history.Append(TargetActivation);
            }
            history.Append(">");
            if (ContainsMetadata(Message.Metadata.TARGET_HISTORY))
            {
                history.Append("    ").Append(GetMetadata(Message.Metadata.TARGET_HISTORY));
            }
            return history.ToString();
        }

        public bool IsSameDestination(IOutgoingMessage other)
        {
            var msg = (Message)other;
            return msg != null && Object.Equals(TargetSilo, msg.TargetSilo);
        }

        // For statistical measuring of time spent in queues.
        private ITimeInterval timeInterval;

        public void Start()
        {
            timeInterval = TimeIntervalFactory.CreateTimeInterval(true);
            timeInterval.Start();
        }

        public void Stop()
        {
            timeInterval.Stop();
        }

        public void Restart()
        {
            timeInterval.Restart();
        }

        public TimeSpan Elapsed
        {
            get { return timeInterval.Elapsed; }
        }


        internal void DropExpiredMessage(MessagingStatisticsGroup.Phase phase)
        {
            MessagingStatisticsGroup.OnMessageExpired(phase);
            if (logger.IsVerbose2) logger.Verbose2("Dropping an expired message: {0}", this);
            ReleaseBodyAndHeaderBuffers();
        }
    }
}
