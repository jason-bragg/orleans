using System;

namespace Orleans.Configuration
{
    public class BackgrounderOptions
    {
        /// <summary>
        /// Max time allowed for background grain operations to complete during grain deactivation.
        /// </summary>
        public TimeSpan BackgroundDeactivationTimeout = DefaultBackgroundDeactivationTimeout;
        public static TimeSpan DefaultBackgroundDeactivationTimeout = TimeSpan.FromSeconds(5);
    }
}
