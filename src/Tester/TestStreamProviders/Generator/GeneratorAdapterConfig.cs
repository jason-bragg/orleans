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

namespace Tester.TestStreamProviders.Generator
{
    public class GeneratorAdapterConfig : IStreamGeneratorConfig
    {
        public const string StreamGeneratorTypeIdName = "StreamGeneratorTypeId";
        public const StreamGeneratorsType StreamGeneratorTypeIdDefault = StreamGeneratorsType.Simple; 
        public StreamGeneratorsType StreamGeneratorTypeId { get; set; }

        private const string StreamProviderNameName = "StreamProviderName";
        public string StreamProviderName { get; set; }

        public int CacheSize { get { return 1024; } }

        private const string TotalQueueCountName = "TotalQueueCount";
        private const int TotalQueueCountDefault = 4;
        public int TotalQueueCount { get; set; }


        protected GeneratorAdapterConfig()
        {
            TotalQueueCount = TotalQueueCountDefault;
        }

        /// <summary>
        /// Utility function to convert config to property bag for use in stream provider configuration
        /// </summary>
        /// <returns></returns>
        protected void WriteProperties(Dictionary<string, string> properties)
        {
            properties.Add(StreamGeneratorTypeIdName, StreamGeneratorTypeId.ToString());
            properties.Add(StreamProviderNameName, StreamProviderName);
            properties.Add(TotalQueueCountName, TotalQueueCount.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Utility function to populate config from provider config
        /// </summary>
        /// <param name="providerConfiguration"></param>
        protected virtual void PopulateFromProviderConfig(IProviderConfiguration providerConfiguration)
        {
            StreamGeneratorTypeId = providerConfiguration.GetEnumProperty(StreamGeneratorTypeIdName, StreamGeneratorTypeIdDefault);
            StreamProviderName = providerConfiguration.GetProperty(StreamProviderNameName, string.Empty);
            if (string.IsNullOrWhiteSpace(StreamProviderName))
            {
                throw new ArgumentOutOfRangeException("providerConfiguration", "StreamProviderName not set.");
            }
            TotalQueueCount = providerConfiguration.GetIntProperty(TotalQueueCountName, TotalQueueCountDefault);
        }
    }
}
