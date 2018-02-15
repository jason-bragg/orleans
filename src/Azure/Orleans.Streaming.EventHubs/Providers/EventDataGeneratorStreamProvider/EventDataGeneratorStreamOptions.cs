using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Options;

namespace Orleans.Hosting
{
    /// <summary>
    /// Setting class for EHGeneratorStreamProvider
    /// </summary>
    public class EventDataGeneratorStreamOptions : EventHubStreamOptions
    {
        /// <summary>
        /// Configure eventhub partition count wanted. EventDataGeneratorStreamProvider would generate the same set of partitions based on the count, when initializing.
        /// For example, if parition count set at 5, the generated partitions will be  partition-0, partition-1, partition-2, partition-3, partiton-4
        /// </summary>
        public int EventHubPartitionCount = DefaultEventHubPartitionCount;
        /// <summary>
        /// Default EventHubPartitionRangeStart
        /// </summary>
        public const int DefaultEventHubPartitionCount = 4;
    }

    public class EventDataGeneratorStreamOptionsFormatterResolver : IOptionFormatterResolver<EventDataGeneratorStreamOptions>
    {
        private IOptionsSnapshot<EventDataGeneratorStreamOptions> optionsSnapshot;

        public EventDataGeneratorStreamOptionsFormatterResolver(IOptionsSnapshot<EventDataGeneratorStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<EventDataGeneratorStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        private class Formatter : IOptionFormatter<EventDataGeneratorStreamOptions>
        {
            private EventDataGeneratorStreamOptions options;
            private readonly EventHubStreamOptionsFormatterResolver.Formatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, EventDataGeneratorStreamOptions options)
            {
                this.options = options;
                this.parentFormatter = new EventHubStreamOptionsFormatterResolver.Formatter(name, options);
                this.Name = OptionFormattingUtilities.Name<EventDataGeneratorStreamOptions>(name);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.Format().ToList();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.EventHubPartitionCount), this.options.EventHubPartitionCount),
                });
                return formatted;
            }
        }
    }
}