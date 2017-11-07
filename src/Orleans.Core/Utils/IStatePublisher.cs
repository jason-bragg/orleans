
namespace Orleans
{
    /// <summary>
    /// State publisher
    /// NOTE: All implementations must be thread safe
    /// NOTE: All state must be immutable
    /// </summary>
    internal interface IStatePublisher<in T>
    {
        /// <summary>
        /// Publish state
        /// </summary>
        /// <param name="state">immutable state to publish</param>
        void Publish(T state);
    }
}
