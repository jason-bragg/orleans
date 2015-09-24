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

namespace Tester.TestStreamProviders.Generator
{
    public class SimpleStreamGeneratorQueue : IStreamGeneratorQueue
    {
        public static readonly string TypeId = typeof(SimpleStreamGeneratorQueue).FullName;
        private ComponentFactory factory;
        private IConfig config;
        private LinkedList<IStreamGenerator> streamGenerators;
        private int sequenceId;
        private int streamsGenerated;
        private DateTime? lastGeneration;

        public interface IConfig : IStreamGeneratorQueueConfig, IStreamGeneratorConfig
        {
            int TotalStreamCountPerQueue { get; }
            int GenerationRateStreamsPerMinute { get; }
        }

        public bool TryReadEvents(DateTime utcNow, out List<GeneratedBatchContainer> events)
        {
            events = new List<GeneratedBatchContainer>();
            if (!GenerateStreams(utcNow) && streamGenerators.Count == 0)
            {
                return false;
            }

            var deadStreams = new List<IStreamGenerator>();
            foreach (IStreamGenerator generator in streamGenerators)
            {
                List<GeneratedBatchContainer> generated;
                if (!generator.TryReadEvents(utcNow, out generated))
                {
                    // stream is done, remove
                    deadStreams.Add(generator);
                    continue;
                }
                events.AddRange(generated);
            }
            deadStreams.ForEach(generator => streamGenerators.Remove(generator));
            events.ForEach(b => b.SequenceToken = new EventSequenceToken(sequenceId++));
            return true;
        }

        private bool GenerateStreams(DateTime utcNow)
        {
            if (streamsGenerated >= config.TotalStreamCountPerQueue)
            {
                return false;
            }

            int numberOfStreamsToCreate;
            if (lastGeneration == null)
            {
                numberOfStreamsToCreate = 1;
            }
            else
            {
                // get time since last read in Ms, cap at 1 second.
                double timeSinceLastReadMs = Math.Min((utcNow - lastGeneration.Value).TotalMilliseconds, 1000.0);
                numberOfStreamsToCreate = (int)Math.Round(config.GenerationRateStreamsPerMinute * timeSinceLastReadMs / 60000.0);
            }

            if (numberOfStreamsToCreate > 0)
            {
                numberOfStreamsToCreate = Math.Min(numberOfStreamsToCreate, config.TotalStreamCountPerQueue - streamsGenerated);
                for (int i = 0; i < numberOfStreamsToCreate; i++)
                {
                    streamGenerators.AddLast(factory.Create<IStreamGenerator>(config.StreamGeneratorTypeId, config));
                }
                lastGeneration = utcNow;
                streamsGenerated += numberOfStreamsToCreate;
            }
            return true;
        }

        public void Configure(ComponentFactory componentFactory, IComponentConfig componentConfig)
        {
            var cfg = componentConfig as IConfig;
            if (cfg == null)
            {
                throw new ArgumentOutOfRangeException("componentConfig");
            }
            config = cfg;
            factory = componentFactory;
            streamGenerators = new LinkedList<IStreamGenerator>();
            sequenceId = 0;
            streamsGenerated = 0;
            lastGeneration = null;
        }
    }
}
