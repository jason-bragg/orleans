using System.Reflection;

namespace Orleans.Runtime
{
    public interface IParameterFacetFactory
    {
        Factory<IGrainActivationContext, object> Create(ParameterInfo parameter, FacetAttribute attribute);
    }

    public interface IParameterFacetFactory<in TAttribute> : IParameterFacetFactory
        where TAttribute : FacetAttribute
    {
        Factory<IGrainActivationContext, object> Create(ParameterInfo parameter, TAttribute attribute);
    }
}
