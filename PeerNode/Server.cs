using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace PeerNode
{
    // Implements the connection logic for the socket server.
    class Server
    {
        Socket listenSocket;            // the socket used to listen for incoming connection requests

        int m_numConnectedSockets;      // the total number of clients connected to the server 
        const int maxMessageSize = 32768;

        // Starts the server such that it is listening for 
        // incoming connection requests.
        //
        // <param name="localEndPoint">The endpoint which the server will listening 
        // for connection requests on</param>
        public void Start(IPEndPoint localEndPoint)
        {
            // create the socket which listens for incoming connections
            listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(localEndPoint);
            // start the server with a listen backlog of 100 connections
            listenSocket.Listen(100);

            // post accepts on the listening socket
            StartAccept(null);
        }


        // Begins an operation to accept a connection request from the client 
        //
        // <param name="acceptEventArg">The context object to use when issuing 
        // the accept operation on the server's listening socket</param>
        public void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (null == acceptEventArg)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(AcceptEventArg_Completed);
            }
            else
            {
                // socket must be cleared since the context object is being reused
                acceptEventArg.AcceptSocket = null;
            }

            bool willRaiseEvent = listenSocket.AcceptAsync(acceptEventArg);

            //Console.WriteLine($"StartAccept {willRaiseEvent}");

            if (!willRaiseEvent)
            {
                ProcessAccept(acceptEventArg);
            }
        }

        // This method is the callback method associated with Socket.AcceptAsync 
        // operations and is invoked when an accept operation is complete
        //
        void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            ProcessAccept(e);
        }

        private void ProcessAccept(SocketAsyncEventArgs e)
        {
            Interlocked.Increment(ref m_numConnectedSockets);
            Console.WriteLine($"Client connection accepted. Connection from: {e.AcceptSocket.RemoteEndPoint}. There are {m_numConnectedSockets} clients connected to the server");

            // Get the socket for the accepted client connection and put it into the 
            //ReadEventArg object user token
            SocketAsyncEventArgs readEventArgs = new SocketAsyncEventArgs();
            readEventArgs.Completed += new EventHandler<SocketAsyncEventArgs>(IO_Completed);
            readEventArgs.SetBuffer(new byte[2 * maxMessageSize], 0, 2 * maxMessageSize);
            readEventArgs.AcceptSocket = e.AcceptSocket;

            // As soon as the client is connected, post a receive to the connection
            bool willRaiseEvent = e.AcceptSocket.ReceiveAsync(readEventArgs);
            if (!willRaiseEvent)
            {
                ProcessReceive(readEventArgs);
            }

            // Accept the next connection request
            StartAccept(e);
        }

        // This method is called whenever a receive or send operation is completed on a socket 
        //
        // <param name="e">SocketAsyncEventArg associated with the completed receive operation</param>
        void IO_Completed(object sender, SocketAsyncEventArgs e)
        {
            // determine which type of operation just completed and call the associated handler
            switch (e.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    //Console.WriteLine($"SocketAsyncOperation.Receive {e.RemoteEndPoint}");
                    ProcessReceive(e);
                    break;
                case SocketAsyncOperation.Send:
                    break;
                default:
                    throw new ArgumentException($"The last operation {e.LastOperation} completed on the socket was not a receive or send");
            }
        }

        public static void ShiftBufferLeft(byte[] bytes, int offset, int len)
        {
            // Iterate through the elements of the array from left to right.
            for (int index = 0; index < len; index++)
            {
                bytes[index] = bytes[index + offset];
            }
        }

        // This method is invoked when an asynchronous receive operation completes. 
        // If the remote host closed the connection, then the socket is closed.  
        // If data was received then the data is echoed back to the client.
        //
        private void ProcessReceive(SocketAsyncEventArgs e)
        {
            // check if the remote host closed the connection
            if (e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                int totalBytesRead = e.Offset + e.BytesTransferred;
                Console.WriteLine("The server has read a total of {0} bytes", totalBytesRead);

                //find end of message
                byte[] buf = e.Buffer;
                int msgSize = Array.IndexOf<byte>(buf, 0, e.Offset, totalBytesRead - e.Offset);
                if (msgSize >= 0)
                {
                    if (null != e.UserToken)
                    {
                        int tail = totalBytesRead - msgSize - 1;
                        ShiftBufferLeft(e.Buffer, msgSize + 1, tail);
                        //discard current message because it is too large
                        e.SetBuffer(tail, maxMessageSize - tail);

                        Console.WriteLine("Message {0} bytes was discarded", (int)e.UserToken + msgSize);

                        totalBytesRead = tail;
                        e.UserToken = null;
                    }

                    //process message
                    if (null == e.UserToken)
                    {
                        int offset = Program.ProcessMessages(e.AcceptSocket.RemoteEndPoint, e.Buffer, totalBytesRead, maxMessageSize);

                        if (offset >= 0)
                        {
                            int tail = totalBytesRead - offset;

                            ShiftBufferLeft(e.Buffer, offset, tail);

                            e.SetBuffer(tail, maxMessageSize - tail);
                        }
                    }
                }
                else
                {
                    if (null == e.UserToken)
                    {
                        if (totalBytesRead >= maxMessageSize)
                        {
                            //discard message
                            e.SetBuffer(0, maxMessageSize);
                            e.UserToken = totalBytesRead;
                        }
                        else
                        {
                            e.SetBuffer(totalBytesRead, maxMessageSize - totalBytesRead);
                        }
                    }
                    else
                    {
                        //discard message
                        e.UserToken = (int)e.UserToken + totalBytesRead;
                        e.SetBuffer(0, maxMessageSize);
                    }
                }

                bool willRaiseEvent = e.AcceptSocket.ReceiveAsync(e);
                if (!willRaiseEvent)
                {
                    ProcessReceive(e);
                }
            }
            else
            {
                CloseClientSocket(e.AcceptSocket);
            }
        }

        /// <summary>
        /// Closes the socket associated with the client
        /// </summary>
        /// <param name="s"></param>
        private void CloseClientSocket(Socket s)
        {
            try
            {
                s.Shutdown(SocketShutdown.Send);
            }
            // throws if client process has already closed
            catch (Exception) { }
            s.Close();

            // decrement the counter keeping track of the total number of clients connected to the server
            Interlocked.Decrement(ref m_numConnectedSockets);

            Console.WriteLine("A client has been disconnected from the server. There are {0} clients connected to the server", m_numConnectedSockets);
        }
    }
}