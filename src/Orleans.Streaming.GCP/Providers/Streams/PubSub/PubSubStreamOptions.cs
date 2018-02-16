using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;

namespace Orleans.Configuration
{
    public class PubSubStreamOptions : PersistentStreamOptions
    {
        public string ProjectId { get; set; }

        public string TopicId { get; set; }

        public string ClusterId { get; set; }

        public string CustomEndpoint { get; set; }

        public int CacheSize { get; set; } = CACHE_SIZE_DEFAULT;
        public const int CACHE_SIZE_DEFAULT = 4096;

        public int NumSubscriptions { get; set; } = NUMBER_SUBSCRIPTIONS_DEFAULT;
        public const int NUMBER_SUBSCRIPTIONS_DEFAULT = 8;

        private TimeSpan? deadline;
        public TimeSpan? Deadline
        {
            get { return this.deadline; }
            set { this.deadline = (value.HasValue) ? TimeSpan.FromTicks(Math.Min(value.Value.Ticks, MAX_DEADLINE.Ticks)) : value; }
        }
        public static readonly TimeSpan MAX_DEADLINE = TimeSpan.FromSeconds(600);
    }

    public class PubSubStreamOptionsFormatterResolver : IOptionFormatterResolver<PubSubStreamOptions>
    {
        private IOptionsSnapshot<PubSubStreamOptions> optionsSnapshot;

        public PubSubStreamOptionsFormatterResolver(IOptionsSnapshot<PubSubStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<PubSubStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        public class Formatter : IOptionFormatter<PubSubStreamOptions>
        {
            private PubSubStreamOptions options;
            private readonly PersistentStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, PubSubStreamOptions options)
            {
                this.options = options;
                this.Name = OptionFormattingUtilities.Name<PubSubStreamOptions>(name);
                this.parentFormatter = new PersistentStreamOptionsFormatter(options);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.FormatSharedOptions();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.ProjectId), this.options.ProjectId),
                    OptionFormattingUtilities.Format(nameof(this.options.TopicId), this.options.TopicId),
                    OptionFormattingUtilities.Format(nameof(this.options.ClusterId), this.options.ClusterId),
                    OptionFormattingUtilities.Format(nameof(this.options.CustomEndpoint), this.options.CustomEndpoint),
                    OptionFormattingUtilities.Format(nameof(this.options.CacheSize), this.options.CacheSize),
                    OptionFormattingUtilities.Format(nameof(this.options.NumSubscriptions), this.options.NumSubscriptions),
                    OptionFormattingUtilities.Format(nameof(this.options.Deadline), this.options.Deadline),
                });
                return formatted;
            }
        }
    }
}
