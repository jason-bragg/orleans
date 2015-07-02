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
using System.Threading.Tasks;

using Orleans.Runtime;
using Orleans.Concurrency;

namespace Orleans.Streams
{
    [Serializable]
    [Immutable]
    public class SubscriptionInfo
    {
        public StreamSequenceToken StreamSequenceToken { get; set; }
    }

    // This is the extension interface for stream consumers
    [Factory(FactoryAttribute.FactoryTypes.ClientObject)]
    internal interface IStreamConsumerExtension : IGrainExtension
    {
        Task DeliverItem(GuidId subscriptionId, Immutable<object> item, StreamSequenceToken token);
        Task DeliverBatch(GuidId subscriptionId, Immutable<IBatchContainer> item);
        Task CompleteStream(GuidId subscriptionId);
        Task ErrorInStream(GuidId subscriptionId, Exception exc);
        Task<SubscriptionInfo> GetSubscriptionInfo(GuidId subscriptionId);
    }

    // This is the extension interface for stream producers
    [Factory(FactoryAttribute.FactoryTypes.ClientObject)]
    internal interface IStreamProducerExtension : IGrainExtension
    {
        [AlwaysInterleave]
        Task AddSubscriber(GuidId subscriptionId, StreamId streamId, IStreamConsumerExtension streamConsumer, StreamSequenceToken token, IStreamFilterPredicateWrapper filter);

        [AlwaysInterleave]
        Task RemoveSubscriber(GuidId subscriptionId, StreamId streamId);
    }
}
