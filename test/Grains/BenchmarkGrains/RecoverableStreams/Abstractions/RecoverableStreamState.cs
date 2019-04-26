using System;

namespace Orleans.Streams
{
    public class RecoverableStreamState<TApplicationState> :
        IEquatable<RecoverableStreamState<TApplicationState>>,
        IComparable<RecoverableStreamState<TApplicationState>>
    {
        /// <summary>
        /// Types of sequence tokens. Used in <see cref="StreamRecoveryState.CompareTo"/>.
        /// </summary>
        private enum StreamSequenceTokenType
        {
            /// <summary>
            /// The token is a Start Token.
            /// </summary>
            Start,

            /// <summary>
            /// The token is a Current token.
            /// </summary>
            CurrentToken,
        }

        public IStreamIdentity StreamId { get; set; }
        public StreamSequenceToken StartToken { get; private set; } // TODO: Saving this in the past has been a nightmare because of abstract base classes. Considering adding "serialize" method to this directly.
        public StreamSequenceToken CurrentToken { get; private set; }
        public bool IsIdle { get; set; }
        public TApplicationState ApplicationState { get; set; }

        /// <summary>
        /// Gets the latest token for the stream 
        /// </summary>
        public StreamSequenceToken GetToken()
        {
            return this.CurrentToken ?? this.StartToken;
        }

        public void SetStartToken(StreamSequenceToken token)
        {
            if (this.StartToken != null)
            {
                throw new InvalidOperationException(FormattableString.Invariant($"Cannot set start token if it is already set. Use the {nameof(this.ResetTokens)} method to reset existing tokens."));
            }

            this.StartToken = token;
        }

        public void SetCurrentToken(StreamSequenceToken token)
        {
            if (this.StartToken == null)
            {
                throw new InvalidOperationException(FormattableString.Invariant($"Cannot set {nameof(this.CurrentToken)} if {nameof(this.StartToken)} is not set. Use the {nameof(this.SetStartToken)} method to set the start token."));
            }

            if (token.EasyCompareTo(this.StartToken) == EasyCompareToResult.Before)
            {
                throw new InvalidOperationException(FormattableString.Invariant($"Cannot set {nameof(this.CurrentToken)} to value before {nameof(this.StartToken)}. {nameof(this.StartToken)}: '{this.StartToken}'. New {nameof(this.CurrentToken)}: '{this.CurrentToken}'."));
            }

            if (token.EasyCompareTo(this.CurrentToken) != EasyCompareToResult.After)
            {
                throw new InvalidOperationException(FormattableString.Invariant($"Cannot set {nameof(this.CurrentToken)} to value that does not come after. Current {nameof(this.CurrentToken)}: '{this.CurrentToken}'. New {nameof(this.CurrentToken)}: '{token}'"));
            }

            this.CurrentToken = this.CurrentToken;
        }

        public void ResetTokens()
        {
            this.StartToken = null;
            this.CurrentToken = null;
        }

        /// <summary>
        /// Determines if an event has been encountered before, as determined by the event's sequence token.
        /// </summary>
        /// <param name="token">The event's sequence token.</param>
        /// <returns>
        /// <see langref="true"/> if the event is considered a duplicate; otherwise, <see langref="false"/>.
        /// </returns>
        public bool IsDuplicateEvent(StreamSequenceToken token)
        {
            if (token == null)
            {
                throw new ArgumentNullException(nameof(token), "It is not possible to determine if an event is duplicate if the token is null");
            }

            // If the Current Token is set, compare against it.
            // When comparing to the Current Token, the event is considered a duplicate if it is before or at the same point.
            if (this.CurrentToken != null)
            {
                return token.EasyCompareTo(this.CurrentToken) != EasyCompareToResult.After;
            }

            // If the Start Token is set, compare against it.
            // When comparing to the Start Token, the event is only considered a duplicate if it is strictly before the Start Token.
            if (this.StartToken != null)
            {
                return token.EasyCompareTo(this.StartToken) == EasyCompareToResult.Before;
            }
            
            // Since neither the Current Token nor the Start Token are set, the event must not be a duplicate.
            return false;
        }

        public bool Equals(RecoverableStreamState<TApplicationState> other) => throw new NotImplementedException();

        public int CompareTo(RecoverableStreamState<TApplicationState> other)
        {
            if (other == null) { throw new ArgumentNullException(nameof(other)); }

            // If my Current Token is set, use that to compare to the other state.
            if (this.CurrentToken != null)
            {
                return CompareMyTokenToOtherState(
                    StreamSequenceTokenType.CurrentToken,
                    this.CurrentToken,
                    other.StartToken,
                    other.CurrentToken);
            }

            // My Current Token wasn't set but if my Start Token is set, use that to compare to the other state.
            if (this.StartToken != null)
            {
                return CompareMyTokenToOtherState(
                    StreamSequenceTokenType.Start,
                    this.StartToken,
                    other.StartToken,
                    other.CurrentToken);
            }

            // My Tokens weren't set. If the other state has Tokens that are set, then I am behind.
            if (other.CurrentToken != null || other.CurrentToken != null)
            {
                return -1;
            }

            // Both the other state and I don't have either token set. Return tied.
            return 0;
        }

        private static int CompareMyTokenToOtherState(
            StreamSequenceTokenType myTokenType,
            StreamSequenceToken myToken,
            StreamSequenceToken otherStartToken,
            StreamSequenceToken otherCurrentToken)
        {
            // If the other state's Current Token is set, compare that to my token.
            if (otherCurrentToken != null)
            {
                return CompareMyTokenToOtherToken(
                    myTokenType,
                    myToken,
                    StreamSequenceTokenType.CurrentToken,
                    otherCurrentToken);
            }

            // The other state's Current Token wasn't set but if its Start Token is, compare that to my token.
            if (otherStartToken != null)
            {
                return CompareMyTokenToOtherToken(
                    myTokenType,
                    myToken,
                    StreamSequenceTokenType.Start,
                    otherStartToken);
            }

            // Neither of the other state's tokens are set. Since my token is set, return ahead.
            return 1;
        }

        private static int CompareMyTokenToOtherToken(
            StreamSequenceTokenType myTokenType,
            StreamSequenceToken myToken,
            StreamSequenceTokenType otherTokenType,
            StreamSequenceToken otherToken)
        {
            var result = myToken.CompareTo(otherToken);

            // In most cases, we can simply directly return the result without considering the types of the tokens.
            // However, if the comparison is equal, then we also must consider the types of the tokens being compared. 
            if (result == 0)
            {
                // If my token is a start token but the other token is a current token, this means that both my state
                // and the other state have seen the same event, but the other state has processed it whereas my
                // state has not. Therefore, I am slightly behind the other state.
                if (myTokenType == StreamSequenceTokenType.Start && otherTokenType == StreamSequenceTokenType.CurrentToken)
                {
                    return -1;
                }

                // If my token is a current token but the other token is a start token, this means that both my state
                // and the other state have seen the same event, but my state has processed it whereas the other state
                // has not. Therefore, I am slightly ahead of the other state.
                if (myTokenType == StreamSequenceTokenType.CurrentToken && otherTokenType == StreamSequenceTokenType.Start)
                {
                    return 1;
                }
            }

            return result;
        }
    }
}