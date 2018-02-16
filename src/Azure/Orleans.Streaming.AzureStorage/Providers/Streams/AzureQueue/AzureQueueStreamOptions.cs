
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Orleans.Runtime.Configuration;

namespace Orleans.Configuration
{
    /// <summary>
    /// Azure queue stream provider options.
    /// </summary>
    public class AzureQueueStreamOptions : PersistentStreamOptions
    {
        public string ConnectionString { get; set; }

        public string ClusterId { get; set; }

        public TimeSpan? MessageVisibilityTimeout { get; set; }

        public int CacheSize { get; set; } = DEFAULT_CACHE_SIZE;
        public const int DEFAULT_CACHE_SIZE = 4096;

        public int NumQueues { get; set; } = DEFAULT_NUM_QUEUES;
        public const int DEFAULT_NUM_QUEUES = 8; // keep as power of 2.
    }

    public class AzureQueueStreamOptionsFormatterResolver : IOptionFormatterResolver<AzureQueueStreamOptions>
    {
        private IOptionsSnapshot<AzureQueueStreamOptions> optionsSnapshot;

        public AzureQueueStreamOptionsFormatterResolver(IOptionsSnapshot<AzureQueueStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<AzureQueueStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        private class Formatter : IOptionFormatter<AzureQueueStreamOptions>
        {
            private AzureQueueStreamOptions options;
            private readonly PersistentStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, AzureQueueStreamOptions options)
            {
                this.options = options;
                this.parentFormatter = new PersistentStreamOptionsFormatter(options);
                this.Name = OptionFormattingUtilities.Name<AzureQueueStreamOptions>(name);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.FormatSharedOptions();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.ConnectionString), ConfigUtilities.RedactConnectionStringInfo(this.options.ConnectionString)),
                    OptionFormattingUtilities.Format(nameof(this.options.ClusterId), this.options.ClusterId),
                    OptionFormattingUtilities.Format(nameof(this.options.MessageVisibilityTimeout), this.options.MessageVisibilityTimeout),
                    OptionFormattingUtilities.Format(nameof(this.options.CacheSize), this.options.CacheSize),
                    OptionFormattingUtilities.Format(nameof(this.options.NumQueues), this.options.NumQueues),
                });
                return formatted;
            }
        }
    }
}
