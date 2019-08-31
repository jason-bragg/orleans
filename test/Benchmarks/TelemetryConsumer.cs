using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Benchmarks
{
    public class TelemetryConsumer : IMetricTelemetryConsumer
    {
        private readonly ILogger logger;

        public TelemetryConsumer(ILogger<TelemetryConsumer> logger)
        {
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public void TrackTrace(string message)
        {
            this.logger.LogInformation(message);
        }

        public void TrackTrace(string message, IDictionary<string, string> properties)
        {
            TrackTrace(PrintProperties(message, properties));
        }

        public void TrackTrace(string message, Severity severity)
        {
            TrackTrace(message);
        }

        public void TrackTrace(string message, Severity severity, IDictionary<string, string> properties)
        {
            TrackTrace(message, properties);
        }

        public void TrackMetric(string name, double value, IDictionary<string, string> properties = null)
        {
            TrackTrace(PrintProperties(name, value, properties));
        }

        public void TrackMetric(string name, TimeSpan value, IDictionary<string, string> properties = null)
        {
            TrackTrace(PrintProperties(name, value, properties));
        }

        public void IncrementMetric(string name)
        {
            TrackTrace(name + $" - Increment");
        }

        public void IncrementMetric(string name, double value)
        {
            TrackTrace(PrintProperties(name, value, null));
        }

        public void DecrementMetric(string name)
        {
            TrackTrace(name + $" - Decrement");
        }

        public void DecrementMetric(string name, double value)
        {
            TrackTrace(PrintProperties(name, value, null));
        }

        public void Flush()
        {
        }

        public void Close()
        {
        }

        private static string PrintProperties<TValue>(string message, TValue value, IDictionary<string, string> properties)
        {
            var sb = new StringBuilder(message + $" - Value: {value}");
            sb = AppendProperties(sb, properties);
            return sb.ToString();
        }

        private static string PrintProperties(string message, IDictionary<string, string> properties)
        {
            var sb = new StringBuilder(message);
            sb = AppendProperties(sb, properties);
            return sb.ToString();
        }

        private static StringBuilder AppendProperties(StringBuilder sb, IDictionary<string, string> properties)
        {
            if (properties == null || properties.Keys.Count == 0)
                return sb;

            sb.Append(" - Properties:");
            sb.Append(" ");
            sb.Append("{");

            foreach (var key in properties.Keys)
            {
                sb.Append(" ");
                sb.Append(key);
                sb.Append(" : ");
                sb.Append(properties[key]);
                sb.Append(",");
            }
            sb.Remove(sb.Length - 1, 1);
            sb.Append(" ");
            sb.Append("}");
            return sb;
        }
    }
}
