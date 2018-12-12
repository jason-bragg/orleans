using System;
using Orleans.Runtime;

namespace Orleans.Pipes.Abstractions
{
    public interface IPullPipeIn<TToken, TData> : IPullPipeIn<string, TToken, TData> { }

    public static class PullPipeGrainConnectorExtensions
    {
        public static IPullPipeIn<string, TToken, TData> Create<TToken, TData>(this IPullPipeGrainConnector connector, string key, GrainReference grainRef, TToken token)
        {
            return connector.Create<string, TToken, TData>(key, grainRef, token);
        }
    }

    public interface IPullPipeOut<TToken, TData> : IPullPipeOut<string, TToken, TData> { }

    public static class PullPipeListenerExtensions
    {
        public static IDisposable Listen<TToken, TData>(this IPullPipeListener listener, Func<string, TToken, IPullPipeOut<string, TToken, TData>> outFactory, Action<string, IDisposable> outRegistery)
        {
            return listener.Listen(outFactory, outRegistery);
        }

        public static IDisposable Listen<TToken, TData>(this IPullPipeListener listener, string key, IPullPipeOut<string, TToken, TData> pullPipeOut)
        {
            return listener.Listen(key, pullPipeOut);
        }
    }
}
