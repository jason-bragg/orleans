
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Orleans.Runtime.Configuration;

namespace Orleans.Configuration
{
    /// <summary>
    /// Simple Azure queue stream provider options.
    /// </summary>
    public class SimpleAzureQueueStreamOptions : PersistentStreamOptions
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }

    public class SimpleAzureQueueOptionsFormatterResolver : IOptionFormatterResolver<SimpleAzureQueueStreamOptions>
    {
        private IOptionsSnapshot<SimpleAzureQueueStreamOptions> optionsSnapshot;

        public SimpleAzureQueueOptionsFormatterResolver(IOptionsSnapshot<SimpleAzureQueueStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<SimpleAzureQueueStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        private class Formatter : IOptionFormatter<SimpleAzureQueueStreamOptions>
        {
            private SimpleAzureQueueStreamOptions options;
            private readonly PersistentStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, SimpleAzureQueueStreamOptions options)
            {
                this.options = options;
                this.parentFormatter = new PersistentStreamOptionsFormatter(options);
                this.Name = OptionFormattingUtilities.Name<SimpleAzureQueueStreamOptions>(name);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.FormatSharedOptions();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.ConnectionString), ConfigUtilities.RedactConnectionStringInfo(this.options.ConnectionString)),
                    OptionFormattingUtilities.Format(nameof(this.options.QueueName), this.options.QueueName),
                });
                return formatted;
            }
        }
    }
}
