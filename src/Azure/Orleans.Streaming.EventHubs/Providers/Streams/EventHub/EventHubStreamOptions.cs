
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;

namespace Orleans.Hosting
{
    /// <summary>
    /// EventHub settings for a specific hub
    /// </summary>
    public class EventHubStreamOptions : RecoverableStreamOptions
    {
        public EventHubStreamOptions()
        {
            base.InitStage = SiloLifecycleStage.ApplicationServices;
            base.StartStage = SiloLifecycleStage.SiloActive;
        }

        /// <summary>
        /// EventHub connection string.
        /// </summary>
        public string ConnectionString { get; set; }
        /// <summary>
        /// EventHub consumer group.
        /// </summary>
        public string ConsumerGroup { get; set; }
        /// <summary>
        /// Hub path.
        /// </summary>
        public string Path { get; set; }
        /// <summary>
        /// Optional parameter that configures the receiver prefetch count.
        /// </summary>
        public int? PrefetchCount { get; set; }
        /// <summary>
        /// In cases where no checkpoint is found, this indicates if service should read from the most recent data, or from the begining of a partition.
        /// </summary>
        public bool StartFromNow { get; set; } = DEFAULT_START_FROM_NOW;
        public const bool DEFAULT_START_FROM_NOW = true;

        /// <summary>
        /// SlowConsumingPressureMonitorConfig
        /// </summary>
        public double? SlowConsumingMonitorFlowControlThreshold { get; set; }

        /// <summary>
        /// SlowConsumingMonitorPressureWindowSize
        /// </summary>
        public TimeSpan? SlowConsumingMonitorPressureWindowSize { get; set; }

        /// <summary>
        /// AveragingCachePressureMonitorFlowControlThreshold, AveragingCachePressureMonitor is turn on by default. 
        /// User can turn it off by setting this value to null
        /// </summary>
        public double? AveragingCachePressureMonitorFlowControlThreshold { get; set; } = DEFAULT_AVERAGING_CACHE_PRESSURE_MONITORING_THRESHOLD;
        public const double AVERAGING_CACHE_PRESSURE_MONITORING_OFF = 1.0;
        public const double DEFAULT_AVERAGING_CACHE_PRESSURE_MONITORING_THRESHOLD = 1.0 / 3.0;

        /// <summary>
        /// Azure table storage connections string.
        /// </summary>
        public string CheckpointDataConnectionString { get; set; }
        /// <summary>
        /// Azure table name.
        /// </summary>
        public string CheckpointTableName { get; set; }
        /// <summary>
        /// Interval to write checkpoints.  Prevents spamming storage.
        /// </summary>
        public TimeSpan CheckpointPersistInterval { get; set; } = DEFAULT_CHECKPOINT_PERSIST_INTERVAL;
        public static readonly TimeSpan DEFAULT_CHECKPOINT_PERSIST_INTERVAL = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Unique namespace for checkpoint data.  Is similar to consumer group.
        /// </summary>
        public string CheckpointNamespace { get; set; }
    }

    public class EventHubStreamOptionsFormatterResolver : IOptionFormatterResolver<EventHubStreamOptions>
    {
        private IOptionsSnapshot<EventHubStreamOptions> optionsSnapshot;

        public EventHubStreamOptionsFormatterResolver(IOptionsSnapshot<EventHubStreamOptions> optionsSnapshot)
        {
            this.optionsSnapshot = optionsSnapshot;
        }

        public IOptionFormatter<EventHubStreamOptions> Resolve(string name)
        {
            return new Formatter(name, optionsSnapshot.Get(name));
        }

        public class Formatter : IOptionFormatter<EventHubStreamOptions>
        {
            private EventHubStreamOptions options;
            private readonly RecoverableStreamOptionsFormatter parentFormatter;

            public string Name { get; }

            public Formatter(string name, EventHubStreamOptions options)
            {
                this.options = options;
                this.Name = OptionFormattingUtilities.Name<EventHubStreamOptions>(name);
                this.parentFormatter = new RecoverableStreamOptionsFormatter(options);
            }

            public IEnumerable<string> Format()
            {
                List<string> formatted = this.parentFormatter.FormatSharedOptions();
                formatted.AddRange(new[]
                {
                    OptionFormattingUtilities.Format(nameof(this.options.ConnectionString), "<-Redacted->"),
                    OptionFormattingUtilities.Format(nameof(this.options.ConsumerGroup), this.options.ConsumerGroup),
                    OptionFormattingUtilities.Format(nameof(this.options.Path), this.options.Path),
                    OptionFormattingUtilities.Format(nameof(this.options.PrefetchCount), this.options.PrefetchCount),
                    OptionFormattingUtilities.Format(nameof(this.options.StartFromNow), this.options.StartFromNow),
                    OptionFormattingUtilities.Format(nameof(this.options.SlowConsumingMonitorFlowControlThreshold), this.options.SlowConsumingMonitorFlowControlThreshold),
                    OptionFormattingUtilities.Format(nameof(this.options.SlowConsumingMonitorPressureWindowSize), this.options.SlowConsumingMonitorPressureWindowSize),
                    OptionFormattingUtilities.Format(nameof(this.options.AveragingCachePressureMonitorFlowControlThreshold), this.options.AveragingCachePressureMonitorFlowControlThreshold),
                    OptionFormattingUtilities.Format(nameof(this.options.CheckpointDataConnectionString), ConfigUtilities.RedactConnectionStringInfo(this.options.CheckpointDataConnectionString)),
                    OptionFormattingUtilities.Format(nameof(this.options.CheckpointTableName), this.options.CheckpointTableName),
                    OptionFormattingUtilities.Format(nameof(this.options.CheckpointPersistInterval), this.options.CheckpointPersistInterval),
                    OptionFormattingUtilities.Format(nameof(this.options.CheckpointNamespace), this.options.CheckpointNamespace),
                });
                return formatted;
            }
        }
    }
}