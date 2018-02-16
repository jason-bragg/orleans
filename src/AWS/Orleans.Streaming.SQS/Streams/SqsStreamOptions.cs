using System.Collections.Generic;
using Microsoft.Extensions.Options;

namespace Orleans.Configuration
{
    public class SqsStreamOptions : PersistentStreamOptions
    {
        public string ClusterId { get; set; }

        public string ConnectionString { get; set; }

        public int CacheSize { get; set; } = CacheSizeDefaultValue;
        public const int CacheSizeDefaultValue = 4096;

        public int NumQueues { get; set; } = NumQueuesDefaultValue;
        public const int NumQueuesDefaultValue = 8; // keep as power of 2.

    }

    public class SqsStreamOptionsFormatterResolver : IOptionFormatterResolver<SqsStreamOptions>
    {
        private IOptionsSnapshot<SqsStreamOptions> optionsSnapshot;

        public SqsStreamOptionsFormatterResolver(IOptionsSnapshot<SqsStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<SqsStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        public class Formatter : IOptionFormatter<SqsStreamOptions>
        {
            private SqsStreamOptions options;
            private readonly PersistentStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, SqsStreamOptions options)
            {
                this.options = options;
                this.Name = OptionFormattingUtilities.Name<SqsStreamOptions>(name);
                this.parentFormatter = new PersistentStreamOptionsFormatter(options);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.FormatSharedOptions();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.ConnectionString), "<--SNIP-->"),
                    OptionFormattingUtilities.Format(nameof(this.options.ClusterId), this.options.ClusterId),
                    OptionFormattingUtilities.Format(nameof(this.options.CacheSize), this.options.CacheSize),
                    OptionFormattingUtilities.Format(nameof(this.options.NumQueues), this.options.NumQueues),
                });
                return formatted;
            }
        }
    }
}
