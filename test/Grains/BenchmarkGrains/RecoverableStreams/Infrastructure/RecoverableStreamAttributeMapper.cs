using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Streams
{
    public class RecoverableStreamAttributeMapper : IAttributeToFactoryMapper<RecoverableStreamAttribute>
    {
        private static readonly MethodInfo create = typeof(IRecoverableStreamFactory).GetMethod("Create");

        public Factory<IGrainActivationContext, object> GetFactory(ParameterInfo parameter, RecoverableStreamAttribute attribute)
        {
            IRecoverableStreamConfiguration config = attribute;
            // use generic type args to define collection type.
            MethodInfo genericCreate = create.MakeGenericMethod(parameter.ParameterType.GetGenericArguments());
            return context => Create(context, genericCreate, config);
        }

        private object Create(IGrainActivationContext context, MethodInfo genericCreate, IRecoverableStreamConfiguration config)
        {
            IRecoverableStreamFactory factory = context.ActivationServices.GetRequiredService<IRecoverableStreamFactory>();
            object[] args = new object[] { context, config };
            return genericCreate.Invoke(factory, args);
        }
    }
}
