
using System;
using System.Collections.Generic;
using System.Reflection;

using Orleans.CodeGeneration;

namespace Orleans.Runtime
{
    /// <summary>
    /// Cache of type metadata.
    /// </summary>
    internal class TypeMetadataCache
    {
        /// <summary>
        /// The mapping between grain types and the corresponding type for the <see cref="IGrainMethodInvoker"/> implementation.
        /// </summary>
        public Dictionary<Type, Type> GrainToInvokerMapping { get; } = new Dictionary<Type, Type>();

        /// <summary>
        /// The mapping between grain types and the corresponding type for the <see cref="GrainReference"/> implementation.
        /// </summary>
        public Dictionary<Type, Type> GrainToReferenceMapping { get; } = new Dictionary<Type, Type>();

        public void FindSupportClasses(Type type)
        {
            var typeInfo = type.GetTypeInfo();
            var invokerAttr = typeInfo.GetCustomAttribute<MethodInvokerAttribute>(false);
            if (invokerAttr != null)
            {
                this.GrainToInvokerMapping.Add(invokerAttr.TargetType, type);
            }

            var grainReferenceAttr = typeInfo.GetCustomAttribute<GrainReferenceAttribute>(false);
            if (grainReferenceAttr != null)
            {
                this.GrainToReferenceMapping.Add(grainReferenceAttr.TargetType, type);
            }
        }
    }
}
