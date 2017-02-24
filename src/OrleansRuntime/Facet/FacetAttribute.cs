
using System;

namespace Orleans.Facet
{
    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Field)]
    public abstract class FacetAttribute : Attribute
    {
        public abstract object Create(IServiceProvider serviceProvider);
    }

    [AttributeUsage(AttributeTargets.Constructor)]
    public class FacetConstructorAttribute : Attribute
    {
    }
}
