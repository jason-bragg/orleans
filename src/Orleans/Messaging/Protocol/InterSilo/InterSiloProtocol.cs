
using System;

namespace Orleans.Messaging.Protocol
{
    internal static class InterSiloProtocol
    {
        public static IInterSiloProtocol V1 => v1.Value;

        private static readonly Lazy<IInterSiloProtocol> v1 = new Lazy<IInterSiloProtocol>(() => new InterSiloProtocol_v1());
    }
}
