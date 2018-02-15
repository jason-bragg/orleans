
using Microsoft.Extensions.Options;
using System.Collections.Generic;

namespace Orleans.Hosting
{
    /// <summary>
    /// This configuration class is used to configure the MemoryStreamProvider.
    /// It tells the stream provider how many queues to create.
    /// </summary>
    public class MemoryStreamOptions : RecoverableStreamOptions
    {
        /// <summary>
        /// Actual total queue count.
        /// </summary>
        public int TotalQueueCount { get; set; } = DEFAULT_TOTAL_QUEUE_COUNT;
        /// <summary>
        /// Total queue count default value.
        /// </summary>
        public const int DEFAULT_TOTAL_QUEUE_COUNT = 4;
    }

    public class MemoryStreamOptionsFormatterResolver : IOptionFormatterResolver<MemoryStreamOptions>
    {
        private IOptionsSnapshot<MemoryStreamOptions> optionsSnapshot;

        public MemoryStreamOptionsFormatterResolver(IOptionsSnapshot<MemoryStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<MemoryStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        private class Formatter : IOptionFormatter<MemoryStreamOptions>
        {
            private MemoryStreamOptions options;
            private readonly RecoverableStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, MemoryStreamOptions options)
            {
                this.options = options;
                this.parentFormatter = new RecoverableStreamOptionsFormatter(options);
                this.Name = OptionFormattingUtilities.Name<MemoryStreamOptions>(name);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.FormatSharedOptions();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.TotalQueueCount), this.options.TotalQueueCount),
                });
                return formatted;
            }
        }
    }
}
