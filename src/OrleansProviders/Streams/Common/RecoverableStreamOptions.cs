﻿using System;
using System.Collections.Generic;

namespace Orleans.Hosting
{
    /// <summary>
    /// Common stream provider settings shared by EventHubStreamProvider, MemoryStreamProvider and GeneratorStreamProvider
    /// </summary>
    public class RecoverableStreamOptions : PersistentStreamOptions
    {
        /// <summary>
        /// Minimum time message will stay in cache before it is available for time based purge.
        /// </summary>
        public TimeSpan DataMinTimeInCache { get; set; } = DefaultDataMinTimeInCache;
        /// <summary>
        /// Drfault DataMinTimeInCache
        /// </summary>
        public static readonly TimeSpan DefaultDataMinTimeInCache = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Difference in time between the newest and oldest messages in the cache.  Any messages older than this will be purged from the cache.
        /// </summary>
        public TimeSpan DataMaxAgeInCache { get; set; } = DefaultDataMaxAgeInCache;
        /// <summary>
        /// Default DataMaxAgeInCache
        /// </summary>
        public static readonly TimeSpan DefaultDataMaxAgeInCache = TimeSpan.FromMinutes(30);


        /// <summary>
        /// Statistic monitor write interval
        /// Statistics generation is triggered by activity.  Interval will be ignored when streams are inactive.
        /// </summary>
        public TimeSpan StatisticMonitorWriteInterval { get; set; } = DefaultStatisticMonitorWriteInterval;
        /// <summary>
        /// Default statistic monitor write interval
        /// </summary>
        public static TimeSpan DefaultStatisticMonitorWriteInterval = TimeSpan.FromMinutes(5);
    }

    public class RecoverableStreamOptionsFormatter
    {
        private RecoverableStreamOptions options;
        private readonly PersistentStreamOptionsFormatter parentFormatter;

        public RecoverableStreamOptionsFormatter(RecoverableStreamOptions options)
        {
            this.options = options;
            this.parentFormatter = new PersistentStreamOptionsFormatter(options);
        }

        public List<string> FormatSharedOptions()
        {
            List<string> formatted = this.parentFormatter.FormatSharedOptions();
            formatted.AddRange(new []
            {
                OptionFormattingUtilities.Format(nameof(this.options.DataMinTimeInCache), this.options.DataMinTimeInCache),
                OptionFormattingUtilities.Format(nameof(this.options.DataMaxAgeInCache), this.options.DataMaxAgeInCache),
                OptionFormattingUtilities.Format(nameof(this.options.StatisticMonitorWriteInterval), this.options.StatisticMonitorWriteInterval),
            });
            return formatted;
        }
    }
}
