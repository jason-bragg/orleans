using System;

namespace Orleans.Streams
{
    [AttributeUsage(AttributeTargets.Parameter)]
    public class RecoverableStreamAttribute : Attribute, IFacetMetadata, IRecoverableStreamConfiguration
    {
        public string StreamProviderName { get; }

        public RecoverableStreamAttribute(string providerName)
        {
            this.StreamProviderName = providerName;
        }
    }
}
