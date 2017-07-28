
using System;
using System.Reflection;
using Orleans.Runtime;

namespace Orleans
{
    /// <summary>
    /// Base class for any attribution of grain constructor facets
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public abstract class GrainConstructorFacetAttribute : Attribute
    {
        /// <summary>
        /// Acquires factory deligate for the type of facet being created.
        /// </summary>
        public abstract Factory<IGrainActivationContext, object> GetFactory(ParameterInfo parameter);
    }
}
