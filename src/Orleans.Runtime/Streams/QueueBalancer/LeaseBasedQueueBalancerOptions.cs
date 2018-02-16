using System;
using System.Collections.Generic;
using Microsoft.Extensions.Options;

namespace Orleans.Configuration
{
    /// <summary>
    /// Config for LeaseBasedQueueBalancer. User need to configure this option in order to use LeaseBasedQueueBalancer in the
    ///   stream provider.  Per stream provider options can be configured as named options using the same name as the provider.
    /// </summary>
    public class LeaseBasedQueueBalancerOptions
    {
        /// <summary>
        /// LeaseProviderType
        /// </summary>
        public Type LeaseProviderType { get; set; }
        /// <summary>
        /// LeaseProviderTypeName
        /// </summary>
        public const string LeaseProviderTypeName = nameof(LeaseProviderType);
        /// <summary>
        /// LeaseLength
        /// </summary>
        public TimeSpan LeaseLength { get; set; } = TimeSpan.FromSeconds(60);
        /// <summary>
        /// LeaseLengthName
        /// </summary>
        public const string LeaseLengthName = nameof(LeaseLengthName);
    }

    public class LeaseBasedQueueBalancerOptionsFormatter : IOptionFormatter<LeaseBasedQueueBalancerOptions>
    {
        public string Name { get; private set; } = nameof(LeaseBasedQueueBalancerOptions);

        private LeaseBasedQueueBalancerOptions options;
        public LeaseBasedQueueBalancerOptionsFormatter(IOptions<LeaseBasedQueueBalancerOptions> options)
        {
            this.options = options.Value;
        }

        public IEnumerable<string> Format()
        {
            return new List<string>()
            {
                OptionFormattingUtilities.Format(nameof(this.options.LeaseProviderType),this.options.LeaseProviderType),
                OptionFormattingUtilities.Format(nameof(this.options.LeaseLength), this.options.LeaseLength)
            };
        }
    }


}
