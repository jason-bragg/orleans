
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using Orleans.CodeGeneration;
using Orleans.Runtime;

namespace Orleans
{
    /// <summary>
    /// Immutable grain type metadata
    /// </summary>
    internal class GrainTypeMetadata
    {
        private static readonly IImmutableDictionary<Type, Type> Empty = Enumerable.Empty<KeyValuePair<Type,Type>>().ToImmutableDictionary();
        private readonly IImmutableDictionary<Type, Type> grainToInvokerMapping;
        private readonly IImmutableDictionary<Type, Type> grainToReferenceMapping;

        public GrainTypeMetadata()
            : this(Empty, Empty)
        {
        }

        public GrainTypeMetadata(IImmutableDictionary<Type, Type> grainToInvokerMapping, IImmutableDictionary<Type, Type> grainToReferenceMapping)
        {
            this.grainToInvokerMapping = grainToInvokerMapping;
            this.grainToReferenceMapping = grainToReferenceMapping;
        }

        public GrainTypeMetadata Append(TypeMetadataCache cache)
        {
            IImmutableDictionary<Type, Type> invokerMapping = Append(grainToInvokerMapping, cache.GrainToInvokerMapping);
            IImmutableDictionary<Type, Type> referenceMapping = Append(grainToReferenceMapping, cache.GrainToReferenceMapping);
            return new GrainTypeMetadata(invokerMapping, referenceMapping);
        }

        public Type GetGrainReferenceType(Type interfaceType)
        {
            var typeInfo = interfaceType.GetTypeInfo();
            CodeGeneratorManager.GenerateAndCacheCodeForAssembly(typeInfo.Assembly);
            var genericInterfaceType = interfaceType.IsConstructedGenericType
                                           ? typeInfo.GetGenericTypeDefinition()
                                           : interfaceType;

            if (!typeof(IAddressable).IsAssignableFrom(interfaceType))
            {
                throw new InvalidCastException(
                    $"Target interface must be derived from {typeof(IAddressable).FullName} - cannot handle {interfaceType}");
            }

            // Try to find the correct GrainReference type for this interface.
            Type grainReferenceType;
            if (!this.grainToReferenceMapping.TryGetValue(genericInterfaceType, out grainReferenceType))
            {
                throw new InvalidOperationException(
                    $"Cannot find generated {nameof(GrainReference)} class for interface '{interfaceType}'");
            }

            if (interfaceType.IsConstructedGenericType)
            {
                grainReferenceType = grainReferenceType.MakeGenericType(typeInfo.GenericTypeArguments);
            }

            if (!typeof(IAddressable).IsAssignableFrom(grainReferenceType))
            {
                // This represents an internal programming error.
                throw new InvalidCastException(
                    $"Target reference type must be derived from {typeof(IAddressable).FullName}- cannot handle {grainReferenceType}");
            }

            return grainReferenceType;
        }

        public Type GetGrainMethodInvokerType(Type interfaceType)
        {
            var typeInfo = interfaceType.GetTypeInfo();
            CodeGeneratorManager.GenerateAndCacheCodeForAssembly(typeInfo.Assembly);
            var genericInterfaceType = interfaceType.IsConstructedGenericType
                                           ? typeInfo.GetGenericTypeDefinition()
                                           : interfaceType;

            // Try to find the correct IGrainMethodInvoker type for this interface.
            Type invokerType;
            if (!this.grainToInvokerMapping.TryGetValue(genericInterfaceType, out invokerType))
            {
                throw new InvalidOperationException(
                    $"Cannot find generated {nameof(IGrainMethodInvoker)} implementation for interface '{interfaceType}'");
            }

            if (interfaceType.IsConstructedGenericType)
            {
                invokerType = invokerType.MakeGenericType(typeInfo.GenericTypeArguments);
            }

            return invokerType;
        }

        private static IImmutableDictionary<Type, Type> Append(IImmutableDictionary<Type, Type> mine, Dictionary<Type, Type> other)
        {
            return other.Count == 0
                ? mine
                : new IReadOnlyDictionary<Type, Type>[] { mine, other }
                    .SelectMany(dict => dict)
                    .ToImmutableDictionary(pair => pair.Key, pair => pair.Value);
        }
    }
}
