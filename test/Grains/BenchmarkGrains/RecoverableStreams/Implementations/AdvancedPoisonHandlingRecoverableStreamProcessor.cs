using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public class AdvancedPoisonHandlingRecoverableStreamProcessor<TState, TEvent> : DecoratingRecoverableStreamProcessorBase<TState, TEvent>
    {
        // We're not sure if StreamSequenceTokens implements GetHashCode()
        private readonly LinkedList<Tuple<StreamSequenceToken, int>> tokenAndCountPairs = new LinkedList<Tuple<StreamSequenceToken, int>>();

        public AdvancedPoisonHandlingRecoverableStreamProcessor(IRecoverableStreamProcessor<TState, TEvent> innerProcessor)
            : base(innerProcessor)
        {
        }

        public override async Task<bool> OnEvent(TState state, StreamSequenceToken token, TEvent @event)
        {
            try
            {
                return await this.InnerProcessor.OnEvent(state, token, @event);
            }
            catch (Exception exception)
            {
                // TODO: Log
                Console.WriteLine(exception);

                // TODO: Find the pair or insert the pair

                // TODO: Check the count. Potentially accumulate the count.

                // TODO: If the count is < threshold, throw. Otherwise call OnError().

                throw;
            }
        }

        public override Task OnPersisted(TState state, StreamSequenceToken token, bool fastForward)
        {
            // TODO: Clear pairs before token

            return this.InnerProcessor.OnPersisted(state, token, fastForward);
        }

        public override Task OnFastForward(TState state, StreamSequenceToken token)
        {
            // TODO: Clear pairs before token

            return this.InnerProcessor.OnFastForward(state, token);
        }
    }
}