using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    internal class StreamObserver<T> : IStreamObserver
    {
        private readonly IAsyncObserver<T> observer;
        private readonly bool isRewindable;
        private StreamHandshakeToken expectedToken;

        public IStreamSubscriptionHandle Handle { get; }

        public StreamObserver(IStreamSubscriptionHandle handle, IAsyncObserver<T> observer, bool isRewindable, StreamSequenceToken token)
        {
            this.Handle = handle ?? throw new ArgumentNullException(nameof(handle));
            this.observer = observer ?? throw new ArgumentNullException(nameof(observer));
            this.isRewindable = isRewindable;
            if (isRewindable)
            {
                expectedToken = StreamHandshakeToken.CreateStartToken(token);
            }
        }

        public StreamHandshakeToken GetSequenceToken()
        {
            return expectedToken;
        }

        public async Task<StreamHandshakeToken> DeliverBatch(IBatchContainer batch, StreamHandshakeToken handshakeToken)
        {
            // we validate expectedToken only for ordered (rewindable) streams
            if (expectedToken != null)
            {
                if (!expectedToken.Equals(handshakeToken))
                    return expectedToken;
            }

            foreach (var itemTuple in batch.GetEvents<T>())
            {
                await NextItem(itemTuple.Item1, itemTuple.Item2);
            }

            if (isRewindable)
            {
                expectedToken = StreamHandshakeToken.CreateDeliveyToken(batch.SequenceToken);
            }
            return null;
        }

        public async Task<StreamHandshakeToken> DeliverItem(object item, StreamSequenceToken currentToken, StreamHandshakeToken handshakeToken)
        {
            if (expectedToken != null)
            {
                if (!expectedToken.Equals(handshakeToken))
                    return expectedToken;
            }

            await NextItem(item, currentToken);

            // check again, in case the expectedToken was changed indiretly via ResumeAsync()
            if (expectedToken != null)
            {
                if (!expectedToken.Equals(handshakeToken))
                    return expectedToken;
            }
            if (isRewindable)
            {
                expectedToken = StreamHandshakeToken.CreateDeliveyToken(currentToken);
            }
            return null;
        }


        private Task NextItem(object item, StreamSequenceToken token)
        {
            T typedItem;
            try
            {
                typedItem = (T)item;
            }
            catch (InvalidCastException)
            {
                // We got an illegal item on the stream -- close it with a Cast exception
                throw new InvalidCastException("Received an item of type " + item.GetType().Name + ", expected " + typeof(T).FullName);
            }

            return observer.OnNextAsync(typedItem, token);
        }

        public Task ErrorInStream(Exception ex)
        {
            return observer == null ? Task.CompletedTask : observer.OnErrorAsync(ex);
        }
    }
}
