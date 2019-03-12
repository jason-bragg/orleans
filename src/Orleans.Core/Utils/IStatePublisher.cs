
namespace Orleans.Runtime
{
    /// <summary>
    /// State publisher
    /// NOTE: All implementations must be thread safe
    /// NOTE: All T must be immutable
    /// </summary>
    public interface IStatePublisher<in T>
    {
        /// <summary>
        /// Publish state
        /// </summary>
        /// <param name="state">immutable state to publish</param>
        void Publish(T state);
    }
}
