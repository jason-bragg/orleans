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
using System.Globalization;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;

namespace Tester.TestStreamProviders.Generator
{
    public class SimpleGeneratorStreamProvider : PersistentStreamProvider<SimpleGeneratorStreamProvider.AdapterFactory>
    {
        public class AdapterFactory : StreamGeneratorAdapterFactory
        {
            public AdapterFactory()
                : base(new StreamGeneratorComponentFactory())
            {
            }

            [Serializable]
            public class Config : SimpleStreamGeneratorQueue.IConfig, SimpleStreamGenerator.IConfig, IConfig
            {
                public bool IsRewindable { get { return true; } }
                public StreamProviderDirection Direction { get { return StreamProviderDirection.ReadOnly; } }
                public string StreamGeneratorQueueTypeId { get { return SimpleStreamGeneratorQueue.TypeId; } }
                public string StreamGeneratorTypeId { get { return SimpleStreamGenerator.TypeId; } }

                private const string StreamProviderNameName = "StreamProviderName";
                public string StreamProviderName { get; set; }
                
                private const string CacheSizeName = "CacheSize";
                private const int CacheSizeDefault = 1024; // 1k
                public int CacheSize { get; set; }

                private const string TotalQueueCountName = "TotalQueueCount";
                private const int TotalQueueCountDefault = 4;
                public int TotalQueueCount { get; set; }

                private const string TotalStreamCountPerQueueName = "TotalStreamCountPerQueue";
                private const int TotalStreamCountPerQueueDefault = 10;
                public int TotalStreamCountPerQueue { get; set; }

                private const string GenerationRateStreamsPerMinuteName = "GenerationRateStreamsPerMinute";
                private const int GenerationRateStreamsPerMinuteDefault = 60; // ~ 1 stream per second
                public int GenerationRateStreamsPerMinute { get; set; }

                private const string StreamNamespaceName = "StreamNamespace";
                public string StreamNamespace { get; set; }

                private const string TotalEventCountPerStreamName = "TotalEventCountPerStream";
                private const int TotalEventCountPerStreamDefault = 100;
                public int TotalEventCountPerStream { get; set; }

                private const string GenerationRateEventsPerSecondName = "GenerationRateEventsPerSecond";
                private const int GenerationRateEventsPerSecondDefault = 10; // ~ 1 event per 100ms
                public int GenerationRateEventsPerSecond { get; set; }

                public Config()
                {
                    CacheSize = CacheSizeDefault;
                    TotalQueueCount = TotalQueueCountDefault;
                    TotalStreamCountPerQueue = TotalStreamCountPerQueueDefault;
                    GenerationRateStreamsPerMinute = GenerationRateStreamsPerMinuteDefault;
                    TotalEventCountPerStream = TotalEventCountPerStreamDefault;
                    GenerationRateEventsPerSecond = GenerationRateEventsPerSecondDefault;
                }


                public IDictionary<string,string> Flatten()
                {
                    var values = new Dictionary<string, string>
                    {
                        {StreamProviderNameName, StreamProviderName},
                        {CacheSizeName, CacheSize.ToString(CultureInfo.InvariantCulture)},
                        {TotalQueueCountName, TotalQueueCount.ToString(CultureInfo.InvariantCulture)},
                        {TotalStreamCountPerQueueName, TotalStreamCountPerQueue.ToString(CultureInfo.InvariantCulture)},
                        {GenerationRateStreamsPerMinuteName, GenerationRateStreamsPerMinute.ToString(CultureInfo.InvariantCulture) },
                        {StreamNamespaceName, StreamNamespace},
                        {TotalEventCountPerStreamName, TotalEventCountPerStream.ToString(CultureInfo.InvariantCulture)},
                        {GenerationRateEventsPerSecondName, GenerationRateEventsPerSecond.ToString(CultureInfo.InvariantCulture)}
                    };
                    return values;
                }

                public void Load(IProviderConfiguration providerConfiguration)
                {
                    StreamProviderName = providerConfiguration.GetStringSetting(StreamProviderNameName, string.Empty);
                    if (string.IsNullOrWhiteSpace(StreamProviderName))
                    {
                        throw new ArgumentOutOfRangeException("providerConfiguration", "StreamProviderName not set.");
                    }
                    CacheSize = providerConfiguration.GetIntSetting(CacheSizeName, CacheSizeDefault);
                    TotalQueueCount = providerConfiguration.GetIntSetting(TotalQueueCountName, TotalQueueCountDefault);
                    TotalStreamCountPerQueue = providerConfiguration.GetIntSetting(TotalStreamCountPerQueueName, TotalStreamCountPerQueueDefault);
                    GenerationRateStreamsPerMinute = providerConfiguration.GetIntSetting(GenerationRateStreamsPerMinuteName, GenerationRateStreamsPerMinuteDefault);
                    StreamNamespace = providerConfiguration.GetStringSetting(StreamNamespaceName, null);
                    TotalEventCountPerStream = providerConfiguration.GetIntSetting(TotalEventCountPerStreamName, TotalEventCountPerStreamDefault);
                    GenerationRateEventsPerSecond = providerConfiguration.GetIntSetting(GenerationRateEventsPerSecondName, GenerationRateEventsPerSecondDefault);
                }
            }

            public override IConfig BuildConfig(IProviderConfiguration providerConfiguration)
            {
                var config = new Config();
                config.Load(providerConfiguration);
                return config;
            }
        }
    }
}
