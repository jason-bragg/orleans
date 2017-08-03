using System.Reflection;

namespace Orleans.Runtime
{
    public interface IParamiterFacetFactory
    {
        Factory<IGrainActivationContext, object> Create(ParameterInfo parameter, FacetAttribute attribute);
    }

    public interface IParamiterFacetFactory<in TAttribute> : IParamiterFacetFactory
        where TAttribute : FacetAttribute
    {
        Factory<IGrainActivationContext, object> Create(ParameterInfo parameter, TAttribute attribute);
    }
}
