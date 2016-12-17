
using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans
{
    /// <summary>
    /// shared grain type metadata
    /// </summary>
    internal interface ISharedGrainTypeMetadata
    {
        GrainTypeMetadata State { get; }
    }

    /// <summary>
    /// Shared grain type metadata publisher
    /// NOTE: All implementations must be thread safe
    /// </summary>
    internal interface ISharedGrainTypeMetadataPublisher : ISharedStatePublisher<GrainTypeMetadata> { }

    internal class SharedGrainTypeMetadataPublisher : ISharedGrainTypeMetadata, ISharedGrainTypeMetadataPublisher
    {
        public GrainTypeMetadata State { get; private set; }
        public void Publish(GrainTypeMetadata state)
        {
            State = state;
        }
    }

    /// <summary>
    /// AssemblyManifest to GrainTypeMetadata bridge for client and test.
    /// </summary>
    internal class GrainTypeMetadataPublisherBridge : IDisposable
    {
        private bool disposed = false;

        public GrainTypeMetadataPublisherBridge(
            ISharedAssemblyManifest assemblyManifest,
            ISharedGrainTypeMetadataPublisher metadataPublisher)
        {
            PublishGrainTypeMetadata(assemblyManifest, metadataPublisher).Ignore();
        }

        private async Task PublishGrainTypeMetadata(ISharedAssemblyManifest assemblyManifest, ISharedGrainTypeMetadataPublisher metadataPublisher)
        {
            var assemblyProcessor = new GrainTypeMetadataAssemblyProcessor(LogManager.GetLogger("GrainTypeMetadataAssemblyProcessor"), assemblyManifest);
            while (!disposed)
            {
                if (await assemblyProcessor.TryProcessNextAssemblies())
                {
                    metadataPublisher.Publish(assemblyProcessor.GrainTypeMetadata);
                }
            }
        }

        public void Dispose()
        {
            disposed = true;
        }
    }
}
