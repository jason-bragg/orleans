
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Orleans.CodeGeneration;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans
{
    internal class GrainTypeMetadataAssemblyProcessor
    {
        private readonly Logger logger;
        private readonly HashSet<Assembly> processedAssemblies;
        private ISharedState<AssemblyManifest> sharedAssemblyManifest;

        public GrainTypeMetadata GrainTypeMetadata { get; private set; }

        public GrainTypeMetadataAssemblyProcessor(Logger logger, ISharedAssemblyManifest sharedAssemblyManifest)
        {
            this.logger = logger;
            this.sharedAssemblyManifest = sharedAssemblyManifest;
            this.GrainTypeMetadata = new GrainTypeMetadata();
            this.processedAssemblies = new HashSet<Assembly>();
            this.TryProcessAssemblies();
        }

        public async Task<bool> TryProcessNextAssemblies()
        {
            this.sharedAssemblyManifest = await sharedAssemblyManifest.NextAsync;
            return this.TryProcessAssemblies();
        }

        private bool TryProcessAssemblies()
        {
            TypeMetadataCache typeCache = null;
            bool assembliesProcessed = false;
            while(true)
            {
                foreach (Assembly assembly in sharedAssemblyManifest.State.Assemblies)
                {
                    assembliesProcessed |= this.TryProcessAssembly(assembly, out typeCache);
                }
                if (!this.sharedAssemblyManifest.NextAsync.IsCompleted)
                    break;
                this.sharedAssemblyManifest = this.sharedAssemblyManifest.NextAsync.Result;
            }
            if (assembliesProcessed)
            {
                this.GrainTypeMetadata = GrainTypeMetadata.Append(typeCache);
            }
            return assembliesProcessed;
        }

        private bool TryProcessAssembly(Assembly assembly, out TypeMetadataCache typeCache, TypeMetadataCache current = null)
        {
            typeCache = current;
            if (assembly == null) return typeCache != null;

            if (!this.processedAssemblies.Add(assembly))
            {
                return typeCache != null;
            }

            string assemblyName = assembly.GetName().Name;
            if (this.logger.IsVerbose3)
            {
                this.logger.Verbose3("Processing assembly {0}", assemblyName);
            }

#if !NETSTANDARD
            // If the assembly is loaded for reflection only avoid processing it.
            if (assembly.ReflectionOnly)
            {
                return typeCache != null;
            }
#endif

            // If the assembly does not reference Orleans, avoid generating code for it.
            if (TypeUtils.IsOrleansOrReferencesOrleans(assembly))
            {
                // Code generation occurs in a self-contained assembly, so invoke it separately.
                var generated = CodeGeneratorManager.GenerateAndCacheCodeForAssembly(assembly);
                this.TryProcessAssembly(generated?.Assembly, out typeCache, typeCache);
            }

            // Process each type in the assembly.
            var assemblyTypes = TypeUtils.GetDefinedTypes(assembly, logger).ToArray();

            // Process each type in the assembly.
            foreach (TypeInfo typeInfo in assemblyTypes)
            {
                try
                {
                    var type = typeInfo.AsType();
                    string typeName = typeInfo.FullName;
                    if (this.logger.IsVerbose3)
                    {
                        this.logger.Verbose3("Processing type {0}", typeName);
                    }

                    SerializationManager.FindSerializationInfo(type);
                    typeCache = typeCache ?? new TypeMetadataCache();
                    typeCache.FindSupportClasses(type);
                }
                catch (Exception exception)
                {
                    this.logger.Error(ErrorCode.SerMgr_TypeRegistrationFailure, "Failed to load type " + typeInfo.FullName + " in assembly " + assembly.FullName + ".", exception);
                }
            }
            return typeCache != null;
        }
    }
}
