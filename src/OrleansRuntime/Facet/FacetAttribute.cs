
using System;

namespace Orleans.Facet
{
    [AttributeUsage(AttributeTargets.Parameter | AttributeTargets.Field)]
    public class FacetAttribute : Attribute
    {
        public string Name { get; }
        public FacetAttribute(string facetTypeName)
        {
            this.Name = facetTypeName;
        }
    }

    [AttributeUsage(AttributeTargets.Constructor)]
    public class FacetConstructorAttribute : Attribute
    {
    }
}
