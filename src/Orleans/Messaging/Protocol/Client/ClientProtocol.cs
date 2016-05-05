
using System;

namespace Orleans.Messaging.Protocol.Client.v1
{
    internal static class ClientProtocol
    {
        public static IClientProtocol V1 => v1.Value;

        private static readonly Lazy<IClientProtocol> v1 = new Lazy<IClientProtocol>(() => new ClientProtocol_v1());
    }
}
