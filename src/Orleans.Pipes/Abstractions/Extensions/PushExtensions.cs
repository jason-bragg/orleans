using System;
using Orleans.Runtime;

namespace Orleans.Pipes.Abstractions
{
    public interface IPushPipeOut<TData> : IPushPipeOut<string, TData> { }

    public static class PushPipeGrainConnectorExtensions
    {
        public static IPushPipeOut<string, TData> Create<TData>(this IPushPipeGrainConnector connector, string key, GrainReference grainRef)
        {
            return connector.Create<string,TData>(key, grainRef);
        }
    }

    public interface IPushPipeIn<TData> : IPushPipeIn<string, TData> { }

    public static class PushPipeListenerExtensions
    {
        public static IDisposable Listen<TData>(this IPushPipeListener listener, Func<string, IPushPipeIn<string, TData>> inFactory, Action<string, IDisposable> inRegistery)
        {
            return listener.Listen(inFactory, inRegistery);
        }

        public static IDisposable Listen<TData>(this IPushPipeListener listener, string key, IPushPipeIn<string, TData> pushPipeIn)
        {
            return listener.Listen(key, pushPipeIn);
        }
    }
}
