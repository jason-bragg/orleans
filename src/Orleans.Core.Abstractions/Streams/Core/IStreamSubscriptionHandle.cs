using Orleans.Runtime;
using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    /// <summary>
    /// Handle representing this subsription.
    /// </summary>
    public interface IStreamSubscriptionHandle
    {
        /// <summary>
        /// Id of stream
        /// </summary>
        IStreamIdentity StreamIdentity { get; }

        /// <summary>
        /// Stream provider name
        /// </summary>
        string ProviderName { get; }

        /// <summary>
        /// Subscription Id
        /// </summary>
        GuidId SubscriptionId { get; }

        /// <summary>
        /// Unsubscribe a stream consumer from this observable.
        /// </summary>
        /// <returns>A promise to unsubscription action.
        /// </returns>
        Task UnsubscribeAsync();

        /// <summary>
        /// Resumed consumption from a subscription to a stream.
        /// </summary>
        /// <param name="observer">The Observer object.</param>
        /// <param name="token">The stream sequence to be used as an offset to start the subscription from.</param>
        /// <returns>A promise with an updates subscription handle.
        /// </returns>
        Task<IStreamSubscriptionHandle> ResumeAsync<T>(IAsyncObserver<T> observer, StreamSequenceToken token = null);
    }
}
