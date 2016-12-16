namespace Orleans.Runtime
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Orleans;
    using Orleans.CodeGeneration;
    using Orleans.Serialization;

    /// <summary>
    /// The assembly processor.
    /// </summary>
    internal class AssemblyProcessor : IDisposable
    {
        /// <summary>
        /// The collection of assemblies which have already been processed.
        /// </summary>
        private readonly HashSet<Assembly> processedAssemblies = new HashSet<Assembly>();

        /// <summary>
        /// The logger.
        /// </summary>
        private readonly Logger logger;
        
        /// <summary>
        /// The initialization lock.
        /// </summary>
        private readonly object initializationLock = new object();

        /// <summary>
        /// The grain type metadata.
        /// </summary>
        private GrainTypeMetadata grainTypeMetadata;

        /// <summary>
        /// Whether or not this class has been initialized.
        /// </summary>
        private bool initialized;

        private readonly ISharedGrainTypeMetadataPublisher grainTypeMetadataPublisher;

        /// <summary>
        /// Initializes a new instance of the <see cref="AssemblyProcessor"/> class.
        /// </summary>
        /// <param name="grainTypeMetadataPublisher">Grain type metadata publisher</param>
        public AssemblyProcessor(ISharedGrainTypeMetadataPublisher grainTypeMetadataPublisher)
        {
            this.grainTypeMetadataPublisher = grainTypeMetadataPublisher;
            this.logger = LogManager.GetLogger("AssemblyProcessor");
            this.grainTypeMetadata = new GrainTypeMetadata();
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        public void Initialize()
        {
            if (this.initialized)
            {
                return;
            }

            lock (this.initializationLock)
            {
                if (this.initialized)
                {
                    return;
                }

                // load the code generator before intercepting assembly loading
                CodeGeneratorManager.Initialize(); 

                // initialize serialization for all assemblies to be loaded.
                AppDomain.CurrentDomain.AssemblyLoad += this.OnAssemblyLoad;

                Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();

                // initialize serialization for already loaded assemblies.
                var generated = CodeGeneratorManager.GenerateAndLoadForAssemblies(assemblies);
                if (generated != null)
                {
                    foreach (var generatedAssembly in generated)
                    {
                        this.ProcessAssembly(generatedAssembly?.Assembly);
                    }
                }

                foreach (var generatedAssembly in CodeGeneratorManager.GetGeneratedAssemblies().Values)
                {
                    this.ProcessAssembly(generatedAssembly?.Assembly);
                }

                foreach (var assembly in assemblies)
                {
                    this.ProcessAssembly(assembly);
                }

                this.initialized = true;
            }
        }

        /// <summary>
        /// Handles <see cref="AppDomain.AssemblyLoad"/> events.
        /// </summary>
        /// <param name="sender">The sender of the event.</param>
        /// <param name="args">The event arguments.</param>
        private void OnAssemblyLoad(object sender, AssemblyLoadEventArgs args)
        {
            ProcessAssembly(args.LoadedAssembly);
        }

        private void ProcessAssembly(Assembly assembly)
        {
            TypeMetadataCache typeCache;
            if (!this.TryProcessAssembly(assembly, out typeCache)) return;
            if (typeCache.GrainToInvokerMapping.Count == 0 && typeCache.GrainToReferenceMapping.Count == 0) return;
            this.grainTypeMetadata = this.grainTypeMetadata.Append(typeCache);
            this.grainTypeMetadataPublisher.Publish(this.grainTypeMetadata.Append(typeCache));
        }

        /// <summary>
        /// Processes the provided assembly.
        /// </summary>
        /// <param name="assembly">The assembly to process.</param>
        /// <param name="typeCache">resulting type cache</param>
        /// <param name="current">staring type cache</param>
        private bool TryProcessAssembly(Assembly assembly, out TypeMetadataCache typeCache, TypeMetadataCache current = null)
        {
            typeCache = current;
            if (assembly == null) return typeCache != null;
             
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

            // Don't bother re-processing an assembly we've already scanned
            lock (this.processedAssemblies)
            {
                if (!this.processedAssemblies.Add(assembly))
                {
                    return typeCache != null;
                }
            }

            // If the assembly does not reference Orleans, avoid generating code for it.
            if (TypeUtils.IsOrleansOrReferencesOrleans(assembly))
            {
                // Code generation occurs in a self-contained assembly, so invoke it separately.
                var generated = CodeGeneratorManager.GenerateAndCacheCodeForAssembly(assembly);
                this.TryProcessAssembly(generated?.Assembly, out typeCache, typeCache);
            }

            // Process each type in the assembly.
            var assemblyTypes = TypeUtils.GetDefinedTypes(assembly, this.logger).ToArray();

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

        /// <summary>
        /// Disposes this instance.
        /// </summary>
        public void Dispose()
        {
            AppDomain.CurrentDomain.AssemblyLoad -= this.OnAssemblyLoad;
        }
    }
}
