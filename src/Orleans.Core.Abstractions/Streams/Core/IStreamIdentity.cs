using System;
using System.Collections.Generic;

namespace Orleans.Streams
{
    public interface IStreamIdentity
    {
        /// <summary> Stream primary key guid. </summary>
        Guid Guid { get; }

        /// <summary> Stream namespace. </summary>
        string Namespace { get; }
    }

    public class StreamIdentityEqualityComparer : IEqualityComparer<IStreamIdentity>
    {
        /// <summary>
        /// Gets the singleton instance of this class.
        /// </summary>
        public static IEqualityComparer<IStreamIdentity> Instance { get; } = new StreamIdentityEqualityComparer();

        public int Compare(IStreamIdentity x, IStreamIdentity y)
        {
            if (x == null && y == null) return 0;
            if (y == null) return -1;
            int diff = x.Guid.CompareTo(y.Guid);
            return (diff != 0) ? diff : string.Compare(x.Namespace, y.Namespace, StringComparison.Ordinal);
        }

        public bool Equals(IStreamIdentity x, IStreamIdentity y)
        {
            if (x == null && y == null) return true;
            if (y == null) return false;
            return x.Guid.Equals(y.Guid) && string.Equals(x.Namespace, y.Namespace, StringComparison.Ordinal);
        }

        public int GetHashCode(IStreamIdentity obj)
        {
            return obj.Guid.GetHashCode() ^ (obj.Namespace != null ? obj.Namespace.GetHashCode() : 0);
        }
    }
}
