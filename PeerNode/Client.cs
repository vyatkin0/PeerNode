using System;
using System.Net;
using System.Net.Sockets;

namespace PeerNode
{
    // Implements the connection logic for the socket client.
    public sealed class SocketClient : IDisposable
    {
        // The socket used to send/receive messages.
        private Socket clientSocket;

        // Flag for connected socket.
        private Boolean connected = false;

        // Listener endpoint.
        private IPEndPoint hostEndPoint;

        // Create an uninitialized client instance.
        // To start the send/receive processing call the
        // Connect method followed by SendReceive method.
        internal SocketClient(String hostName, Int32 port)
        {
            // Get host related information.
            IPHostEntry host = Dns.GetHostEntry(hostName);

            // Address of the host.
            IPAddress[] addressList = host.AddressList;

            // Instantiates the endpoint and socket.
            hostEndPoint =
              new IPEndPoint(addressList[addressList.Length - 1], port);
            clientSocket = new Socket(hostEndPoint.AddressFamily,
                               SocketType.Stream, ProtocolType.Tcp);
        }

        // Connect to the host.
        internal void Connect()
        {
            SocketAsyncEventArgs connectArgs = new SocketAsyncEventArgs();

            connectArgs.UserToken = clientSocket;
            connectArgs.RemoteEndPoint = hostEndPoint;
            connectArgs.Completed += new EventHandler<SocketAsyncEventArgs> (OnConnect);

            Console.WriteLine($"Connecting to {hostEndPoint.Address} {hostEndPoint.Port}");

            if(!clientSocket.ConnectAsync(connectArgs))
                Console.WriteLine($"Connected to {hostEndPoint.Address} {hostEndPoint.Port} {connectArgs.SocketError}");
        }

        // Calback for connect operation
        private void OnConnect(object sender, SocketAsyncEventArgs e)
        {
            // Set the flag for socket connected.
            connected = (e.SocketError == SocketError.Success);

            if(connected)
                Console.WriteLine($"Connected to {hostEndPoint.Address} {hostEndPoint.Port} {e.SocketError}");
        }

        // Calback for send operation
        private void OnSend(object sender, SocketAsyncEventArgs e)
        {
            //Console.WriteLine($"OnSend {e.LastOperation} {e.SocketError} {e.BytesTransferred}");

            if (e.SocketError != SocketError.Success)
            {
                ProcessError(e);
            }
        }

        // Close socket in case of failure and throws
        // a SockeException according to the SocketError.
        private void ProcessError(SocketAsyncEventArgs e)
        {
            Socket s = e.UserToken as Socket;
            if (s.Connected)
            {
                // close the socket associated with the client
                try
                {
                    s.Shutdown(SocketShutdown.Both);
                }
                catch (Exception)
                {
                    // throws if client process has already closed
                }
                finally
                {
                    if (s.Connected)
                    {
                        s.Close();
                    }
                }
            }

            // Throw the SocketException
            throw new SocketException((Int32)e.SocketError);
        }

        // Exchange a message with the host.
        internal bool Send(byte[] buffer)
        {
            return Send(buffer, 0, buffer.Length);
        }

         internal bool Send(byte[] buffer, int offset, int size)
        {
            if (connected)
            {
                // Prepare arguments for send/receive operation.
                SocketAsyncEventArgs completeArgs = new SocketAsyncEventArgs();
                completeArgs.SetBuffer(buffer, offset, size);
                completeArgs.UserToken = clientSocket;
                //completeArgs.RemoteEndPoint = hostEndPoint;
                completeArgs.Completed += new EventHandler<SocketAsyncEventArgs>(OnSend);

                // Start sending asynchronously.
                bool result = clientSocket.SendAsync(completeArgs);

                //Console.WriteLine($"Send async {completeArgs.BytesTransferred} {result}");

                return result;
            }
            else
            {
                throw new SocketException((Int32)SocketError.NotConnected);
            }
        }

        // Disposes the instance of SocketClient.
        public void Dispose()
        {
            clientSocket.Close();
        }
    }
}