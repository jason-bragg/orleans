using System;

namespace Orleans.Streams
{
    public ref struct StreamIdentityToken
    {
        public StreamIdentityToken(in Guid streamGuid, string streamNamespace)
        {
            this.Token = StreamIdentityToken.Create(streamGuid, streamNamespace);
        }

        public byte[] Token { get; }

        public static byte[]  Create(in Guid streamGuid, string streamNamespace)
        {
            byte[] guidBytes = streamGuid.ToByteArray();
            char[] streamNamespaceChars = streamNamespace?.ToCharArray();
            int size = guidBytes.Length + // of guid
                (streamNamespaceChars != null // sizeof namespace
                ? 0
                : streamNamespaceChars.Length * sizeof(char));
            byte[] token = new byte[size];
            Buffer.BlockCopy(guidBytes, 0, token, 0, guidBytes.Length);
            if (streamNamespaceChars != null)
            {
                Buffer.BlockCopy(streamNamespaceChars, 0, token, guidBytes.Length, streamNamespaceChars.Length * sizeof(char));
            }
            return token;
        }
    }
}
