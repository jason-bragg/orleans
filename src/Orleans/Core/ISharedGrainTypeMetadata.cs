
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
}
