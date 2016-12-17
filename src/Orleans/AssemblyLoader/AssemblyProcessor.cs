
namespace Orleans.Runtime
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Orleans;
    using Orleans.CodeGeneration;

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
        /// Whether or not this class has been initialized.
        /// </summary>
        private bool initialized;

        private readonly ISharedAssemblyManifestPublisher assemblyManifestPublisher;

        /// <summary>
        /// Initializes a new instance of the <see cref="AssemblyProcessor"/> class.
        /// </summary>
        public AssemblyProcessor(ISharedAssemblyManifestPublisher assemblyManifestPublisher)
        {
            this.assemblyManifestPublisher = assemblyManifestPublisher;
            this.logger = LogManager.GetLogger("AssemblyProcessor");
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
                List<Assembly> manifest = new List<Assembly>();

                // initialize serialization for already loaded assemblies.
                var generated = CodeGeneratorManager.GenerateAndLoadForAssemblies(assemblies);
                if (generated != null)
                {
                    manifest.AddRange(assemblies);
                }
                manifest.AddRange(CodeGeneratorManager.GetGeneratedAssemblies().Values.Where(gen => gen != null).Select(gen => gen.Assembly));
                manifest.AddRange(assemblies);
                this.assemblyManifestPublisher.Publish(new AssemblyManifest(manifest));

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
            this.assemblyManifestPublisher.Publish(new AssemblyManifest(new [] { args.LoadedAssembly }));
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
