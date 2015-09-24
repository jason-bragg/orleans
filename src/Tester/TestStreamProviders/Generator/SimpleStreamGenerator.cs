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
using Orleans.Providers.Streams.Common;
using TestGrains;

namespace Tester.TestStreamProviders.Generator
{
    public class SimpleStreamGenerator : IStreamGenerator
    {
        public static readonly string TypeId = typeof(SimpleStreamGenerator).FullName;
        private IConfig config;
        private Guid streamGuid;
        private DateTime? lastGeneration;
        private int eventsGenerated;

        public interface IConfig : IStreamGeneratorConfig
        {
            string StreamNamespace { get; }
            int TotalEventCountPerStream { get; }
            int GenerationRateEventsPerSecond { get; }
        }

        public bool TryReadEvents(DateTime utcNow, out List<GeneratedBatchContainer> events)
        {
            events = new List<GeneratedBatchContainer>();
            if (eventsGenerated >= config.TotalEventCountPerStream)
            {
                return false;
            }

            int numberOfMessagesToRequest;
            if (lastGeneration == null)
            {
                numberOfMessagesToRequest = 1;
            }
            else
            {
                // get time since last read in Ms, cap at 1 second.
                double timeSinceLastReadMs = Math.Min((utcNow - lastGeneration.Value).TotalMilliseconds, 1000.0);
                numberOfMessagesToRequest = (int)Math.Round(config.GenerationRateEventsPerSecond * timeSinceLastReadMs / 1000.0);
            }

            if (numberOfMessagesToRequest > 0)
            {
                numberOfMessagesToRequest = Math.Min(numberOfMessagesToRequest, config.TotalEventCountPerStream - eventsGenerated);
                for (int i = 0; i < numberOfMessagesToRequest; i++)
                {
                    events.Add(GenerateBatch());
                }
                lastGeneration = utcNow;
            }

            return true;
        }

        private GeneratedBatchContainer GenerateBatch()
        {
            eventsGenerated++;
            var evt = new GeneratedEvent
            {
                EventType = (eventsGenerated != config.TotalEventCountPerStream)
                        ? GeneratedEvent.GeneratedEventType.Fill
                        : GeneratedEvent.GeneratedEventType.End,
            };
            return new GeneratedBatchContainer(streamGuid, config.StreamNamespace, evt);
        }

        public void Configure(ComponentFactory componentFactory, IComponentConfig componentConfig)
        {
            var cfg = componentConfig as IConfig;
            if (cfg == null)
            {
                throw new ArgumentOutOfRangeException("componentConfig");
            }
            config = cfg;
            streamGuid = Guid.NewGuid();
            lastGeneration = null;
            eventsGenerated = 0;
        }
    }
}
