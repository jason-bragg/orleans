
using System.Collections.Generic;
using Microsoft.Extensions.Options;

namespace Orleans.Hosting
{
    /// <summary>
    /// This configuration class is used to configure the GeneratorStreamProvider.
    /// It tells the stream provider how many queues to create, and which generator to use to generate event streams.
    /// </summary>
    public class GeneratedStreamOptions : RecoverableStreamOptions
    {
        /// <summary>
        /// Total number of queues
        /// </summary>
        public int TotalQueueCount { get; set; } = DEFAULT_TOTAL_QUEUE_COUNT;
        public const int DEFAULT_TOTAL_QUEUE_COUNT = 4;
    }

    public class GeneratedStreamOptionsFormatterResolver : IOptionFormatterResolver<GeneratedStreamOptions>
    {
        private IOptionsSnapshot<GeneratedStreamOptions> optionsSnapshot;
        public GeneratedStreamOptionsFormatterResolver(IOptionsSnapshot<GeneratedStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<GeneratedStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        private class Formatter : IOptionFormatter<GeneratedStreamOptions>
        {
            private GeneratedStreamOptions options;
            private readonly RecoverableStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, GeneratedStreamOptions options)
            {
                this.options = options;
                this.parentFormatter = new RecoverableStreamOptionsFormatter(options);
                this.Name = OptionFormattingUtilities.Name<GeneratedStreamOptions>(name);
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
