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
using System.Threading.Tasks;
using Orleans.Streams;

namespace Orleans.Providers.Streams.Common
{
    public interface IQueueAdapterShellReceiverFactory : IFactory<QueueId, IQueueAdapterReceiver>
    {
    }

    public interface IQueueAdapterShellSender
    {
        Task EnqueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext);
    }

    public class ReadOnlyQueueAdapterShellSender : IQueueAdapterShellSender
    {
        public Task EnqueueMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
        Dictionary<string, object> requestContext)
        {
            throw new InvalidOperationException("Stream provider is readonly");
        }
    }
    
    public class QueueAdapterShell : IQueueAdapter
    {
        private readonly IConfig config;
        private readonly IQueueAdapterShellSender sender;
        private readonly IQueueAdapterShellReceiverFactory receiverFactory;

        public string Name { get { return config.StreamProviderName; } }

        public interface IConfig
        {
            string StreamProviderName { get; }
            bool IsRewindable { get; }
            StreamProviderDirection Direction { get; }
        }

        public QueueAdapterShell(IConfig config, IQueueAdapterShellSender sender, IQueueAdapterShellReceiverFactory receiverFactory)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }
            if (sender == null)
            {
                throw new ArgumentNullException("sender");
            }
            if (receiverFactory == null)
            {
                throw new ArgumentNullException("receiverFactory");
            }
            this.config = config;
            this.sender = sender;
            this.receiverFactory = receiverFactory;
        }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            if (config.Direction != StreamProviderDirection.ReadOnly)
            {
                sender.EnqueueMessage(streamGuid, streamNamespace, events, token, requestContext);
            }
            return TaskDone.Done;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return receiverFactory.Create(queueId);
        }

        public bool IsRewindable { get { return config.IsRewindable; } }
        public StreamProviderDirection Direction { get { return config.Direction; } }
    }
}
