
using System;
using System.Net;
using System.Net.Sockets;

namespace Orleans.Runtime
{
    internal class SocketManager
    {
        internal static void WriteConnectionPreemble(Socket socket, GrainId grainId)
        {
            int size = 0;
            byte[] grainIdByteArray = null;
            if (grainId != null)
            {
                grainIdByteArray = grainId.ToByteArray();
                size += grainIdByteArray.Length;
            }
            ByteArrayBuilder sizeArray = new ByteArrayBuilder();
            sizeArray.Append(size);
            socket.Send(sizeArray.ToBytes());       // The size of the data that is coming next.
            //socket.Send(guid.ToByteArray());        // The guid of client/silo id
            if (grainId != null)
            {
                // No need to send in a loop.
                // From MSDN: If you are using a connection-oriented protocol, Send will block until all of the bytes in the buffer are sent, 
                // unless a time-out was set by using Socket.SendTimeout. 
                // If the time-out value was exceeded, the Send call will throw a SocketException. 
                socket.Send(grainIdByteArray);     // The grainId of the client
            }
        }

        /// <summary>
        /// Creates a socket bound to an address for use accepting connections.
        /// This is for use by client gateways and other acceptors.
        /// </summary>
        /// <param name="address">The address to bind to.</param>
        /// <returns>The new socket, appropriately bound.</returns>
        internal static Socket GetAcceptingSocketForEndpoint(IPEndPoint address)
        {
            var s = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // Prep the socket so it will reset on close
                s.LingerState = new LingerOption(true, 0);
                s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                // And bind it to the address
                s.Bind(address);
            }
            catch (Exception)
            {
                CloseSocket(s);
                throw;
            }
            return s;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static void CloseSocket(Socket s)
        {
            if (s == null)
            {
                return;
            }

            try
            {
                s.Shutdown(SocketShutdown.Both);
            }
            catch (ObjectDisposedException)
            {
                // Socket is already closed -- we're done here
                return;
            }
            catch (Exception)
            {
                // Ignore
            }

            try
            {
                s.Disconnect(false);
            }
            catch (Exception)
            {
                // Ignore
            }

            try
            {
                s.Dispose();
            }
            catch (Exception)
            {
                // Ignore
            }
        }
    }
}
