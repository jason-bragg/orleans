using System;
using System.Threading.Tasks;

namespace Orleans.Streams
{
    public class BasicPoisonHandlingRecoverableStreamProcessor<TState, TEvent> : DecoratingRecoverableStreamProcessorBase<TState, TEvent>
    {
        private StreamSequenceToken token;
        private int count;

        public BasicPoisonHandlingRecoverableStreamProcessor(IRecoverableStreamProcessor<TState, TEvent> innerProcessor)
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

                if (this.token == null)
                {
                    this.token = token;
                    this.count = 1;
                    throw;
                }

                var comparison = token.EasyCompareTo(this.token);

                switch (comparison)
                {
                    case EasyCompareToResult.Before:
                    {
                        throw;
                    }
                    case EasyCompareToResult.Equal:
                    {
                        ++this.count;

                        if (this.count < 10) // TODO: Config
                        {
                            throw;
                        }

                        // TODO: Error handling
                        return await this.OnError(state, token, exception, new PoisonEventErrorArgs());
                    }
                    case EasyCompareToResult.After:
                    {
                        this.token = token;
                        this.count = 1;

                        throw;
                    }
                    default:
                    {
                        throw new NotSupportedException($"Unknown {nameof(EasyCompareToResult)} '{comparison}'");
                    }
                }
            }
        }
    }
}