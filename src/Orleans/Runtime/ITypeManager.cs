
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    /// <summary>
    /// Client gateway interface for obtaining the grain interface/type map.
    /// </summary>
    internal interface ITypeManager : ISystemTarget
    {
        /// <summary>
        /// Acquires grain interface map for all grain types supported across the entire cluster
        /// </summary>
        /// <returns></returns>
        Task<IGrainTypeResolver> GetClusterTypeCodeMap();

        /// <summary>
        /// Acquires grain interface map for all grain types supported by hosted silo.
        /// </summary>
        /// <returns></returns>
        Task<GrainInterfaceMap> GetSiloTypeCodeMap();

        Task<Streams.ImplicitStreamSubscriberTable> GetImplicitStreamSubscriberTable(SiloAddress silo);
    }
}
