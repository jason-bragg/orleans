
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Orleans
{
    internal class AssemblyManifest
    {
        public AssemblyManifest(IEnumerable<Assembly> assemblies)
        {
            this.Assemblies = assemblies.ToList().AsReadOnly();
        }

        public IReadOnlyList<Assembly> Assemblies { get; }
    }

    internal interface ISharedAssemblyManifest : ISharedState<AssemblyManifest> { }
    internal class SharedAssemblyManifest : SharedState<AssemblyManifest>, ISharedAssemblyManifest
    {
        public SharedAssemblyManifest(SharedState<AssemblyManifest> sharedState) : base(sharedState) { }
    }

    internal interface ISharedAssemblyManifestPublisher : ISharedStatePublisher<AssemblyManifest> { }
    internal class SharedAssemblyManifestPublisher : SharedStatePublisherBase<AssemblyManifest>, ISharedAssemblyManifestPublisher
    {
        private static readonly AssemblyManifest Empty = new AssemblyManifest(Enumerable.Empty<Assembly>());

        public ISharedAssemblyManifest State { get; }

        public SharedAssemblyManifestPublisher() : base(Empty)
        {
            // User requested state aways starts at the front of the chain, as assemblies are deltas
            this.State = new SharedAssemblyManifest(currentState);
        }
    }
}
