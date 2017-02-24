
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Orleans.Factory;

namespace Orleans.Facet
{
    class FacetedConstructorInfo
    {
        private static readonly List<Func<IServiceProvider, object>> EmptyFactories = new List<Func<IServiceProvider, object>>();
        private static readonly Type[] EmptyParameters = new Type[0];
        private readonly List<Func<IServiceProvider, object>> constructorParameterFactories;

        public Type[] FacetParameters { get; }

        public FacetedConstructorInfo(Type type)
        {
            ConstructorInfo constructor = FindConstructor(type);
            this.constructorParameterFactories = CreateConstructorParameterFactories(constructor);
            this.FacetParameters = GetConstructorFacetParameters(constructor);
        }

        public object[] CreateConstructorParameterFacets(IServiceProvider serviceProvider)
        {
            return this.constructorParameterFactories.Select(factory => factory.Invoke(serviceProvider)).ToArray();
        }

        private static ConstructorInfo FindConstructor(Type type)
        {
            ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
            return constructors.Length < 2
                ? constructors.FirstOrDefault()
                : constructors.FirstOrDefault(c => c.GetCustomAttribute<FacetConstructorAttribute>() != null);
        }

        private static Type[] GetConstructorFacetParameters(ConstructorInfo constructor)
        {
            return constructor == null
                ? EmptyParameters
                : constructor
                    .GetParameters()
                    .Where(pi => typeof(IGrainFacet).IsAssignableFrom(pi.ParameterType))
                    .Select(pi => pi.ParameterType)
                    .ToArray();
        }


        private static List<Func<IServiceProvider, object>> CreateConstructorParameterFactories(ConstructorInfo constructor)
        {
            return constructor == null
                ? EmptyFactories
                : constructor
                    .GetParameters()
                    .Where(pi => typeof(IGrainFacet).IsAssignableFrom(pi.ParameterType))
                    .Select(CreateConstructorParameterFactory)
                    .ToList();
        }

        private static Func<IServiceProvider, object> CreateConstructorParameterFactory(ParameterInfo parameter)
        {
            FacetAttribute attribute = parameter.GetCustomAttribute<FacetAttribute>();
            Type factoryType = attribute == null
                ? FactoryTypeFactory.CreateFactoryInterface(parameter.ParameterType)
                : FactoryTypeFactory.CreateFactoryInterface(typeof(string), parameter.ParameterType);
            MethodInfo createFn = factoryType.GetMethod("Create");
            return sp =>
            {
                object factory = sp.GetService(factoryType);
                object[] args = attribute == null ? null : new object[] { attribute.Name };
                return createFn.Invoke(factory, args);
            };
        }
    }
}
