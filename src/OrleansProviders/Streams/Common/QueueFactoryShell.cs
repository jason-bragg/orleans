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
using Orleans.Streams;

namespace Orleans.Providers.Streams.Common
{
    public class QueueFactoryShell
    {
        private readonly IQueueAdapter queueAdapter;
        private readonly IQueueAdapterCache queueAdapterCache;
        private readonly IStreamQueueMapper streamQueueMapper;
        private readonly IFactory<QueueId, IStreamFailureHandler> streamFailureHandlerFactory;

        public QueueFactoryShell(IQueueAdapter queueAdapter, IQueueAdapterCache queueAdapterCache, IStreamQueueMapper streamQueueMapper, IFactory<QueueId, IStreamFailureHandler> streamFailureHandlerFactory)
        {
            if (queueAdapter == null)
            {
                throw new ArgumentNullException("queueAdapter");
            }
            if (queueAdapterCache == null)
            {
                throw new ArgumentNullException("queueAdapterCache");
            }
            if (streamQueueMapper == null)
            {
                throw new ArgumentNullException("streamQueueMapper");
            }
            if (streamFailureHandlerFactory == null)
            {
                throw new ArgumentNullException("streamFailureHandlerFactory");
            }
            this.queueAdapter = queueAdapter;
            this.queueAdapterCache = queueAdapterCache;
            this.streamQueueMapper = streamQueueMapper;
            this.streamFailureHandlerFactory = streamFailureHandlerFactory;
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            return Task.FromResult(queueAdapter);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return queueAdapterCache;
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return streamQueueMapper;
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult(streamFailureHandlerFactory.Create(queueId));
        }
    }
}
