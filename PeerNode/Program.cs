using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;

/// <summary>
/// Simple peer node
/// </summary>
namespace PeerNode
{
    [DataContract]
    public class PeerNode
    {
        [DataMember(Name = "id")]
        public string Id;//Node identifier

        [DataMember(Name = "address")]
        public string Address;//Ip address

        [DataMember(Name = "port")]
        public int Port;//TCP/IP port

        [IgnoreDataMember]
        public SocketClient client;//Interface to send data to node

        [IgnoreDataMember]
        public int seqNum;//Last transaction number for node
    }

    [DataContract]
    public class Config
    {
        [DataMember(Name = "node")]
        public PeerNode Node;//Current node

        [DataMember(Name = "network")]
        public PeerNode[] network;//List of all nodes in the peer network

        [DataMember(Name = "previous_node")]
        public PeerNode prevNode;//Previous node

        [DataMember(Name = "block_timeout")]
        public int blockTimeout;

        [DataMember(Name = "transaction_timeout")]
        public int tranTimeout;
    }

    [DataContract]
    class TransactionBlock
    {
        [DataMember(Name = "node_id")]
        public string nodeId;//Node id that creates the transaction block

        [DataMember(Name = "timestamp")]
        public DateTime timeStamp;//Time when block was created

        [DataMember(Name = "transactions")]
        public ReceivedTransaction[] transactions;//Transactions of block
    }

    [DataContract]
    class ReceivedTransactionBlock
    {
        [DataMember(Name = "block")]
        public TransactionBlock transactionBlock;//received transaction block

        [DataMember(Name = "timestamp")]
        public DateTime timeStamp;//Time when block was received
    }

    [DataContract]
    class Transaction
    {
        [DataMember(Name = "node_id")]
        public string nodeId;//Node id that creates the transaction

        [DataMember(Name = "seq_number")]
        public int seqNum;//Transaction number

        [DataMember(Name = "timestamp")]
        public DateTime timeStamp;//Time when transaction was created
    }

    [DataContract]
    class ReceivedTransaction
    {
        [DataMember(Name = "node_id")]
        public string id;//Node id that received the transaction

        [DataMember(Name = "data")]
        public Transaction transaction;//received transaction

        [DataMember(Name = "count")]
        public int count;//Number of times transaction has been received by node

        [DataMember(Name = "timestamp")]
        public DateTime timeStamp;//Time when transaction was received by node
    }

    class Program
    {
        public static Config AppConfig;

        /// <summary>
        /// List of received transactions
        /// </summary>
        public static List<ReceivedTransaction> receivedTransactions = new List<ReceivedTransaction>();

        static void Main(string[] args)
        {
            try
            {
                var serializer = new DataContractJsonSerializer(typeof(Config));

                using (FileStream fs = new FileStream("appsettings.json", FileMode.Open))
                {
                    AppConfig = serializer.ReadObject(fs) as Config;

                    if (null == AppConfig.Node) throw (new Exception("Node is not specified in configuration file"));

                    if (null == AppConfig.Node.Id) throw (new Exception("Mail 'user' is not specified in configuration file"));
                    if (null == AppConfig.Node.Address) throw (new Exception("Mail 'password' is not specified in configuration file"));
                    if (0 == AppConfig.Node.Port) throw (new Exception("Mail 'user' is not specified in configuration file"));

                    //if (null == AppConfig.prevNode) throw (new Exception("Previous node is not specified in configuration file"));
                    if (null == AppConfig.network) throw (new Exception("Network is not specified in configuration file"));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return;
            }

            Console.WriteLine($"Node started id={AppConfig.Node.Id} previous={AppConfig.prevNode?.Id}");

            Server s = new Server();

            IPAddress ip;

            if(!IPAddress.TryParse(AppConfig.Node.Address, out ip))
            {
                Console.WriteLine("Unable to parse \"{0}\" to a valid IP address!", AppConfig.Node.Address);
                return;
            }

            IPEndPoint endpoint = new IPEndPoint(ip, AppConfig.Node.Port);

            //Start server to receive transactions and blocks
            s.Start(endpoint);

            Console.WriteLine("Server started!");
            Console.WriteLine("Press any key to start connections to peers.");
            Console.ReadKey(true);

            //Initialize connections to remote peers to send transactions and blocks
            foreach (PeerNode p in AppConfig.network)
            {
                if (p.Id == AppConfig.Node.Id) continue;
                p.client = new SocketClient(p.Address, p.Port);
                p.client.Connect();
            }

            Console.WriteLine("Press (1) to send a transaction, (2) to send a block, (Esc) key to stop...");

            ConsoleKey key = Console.ReadKey(true).Key;

            Timer t1 = null;
            Timer t2 = null;

            if (key == ConsoleKey.D3)
            {
                t1 = new Timer((o)=>SendTransaction(), null, 0, AppConfig.tranTimeout);
                t2 = new Timer((o)=>SendTransactionBlock(), null, 1000, AppConfig.blockTimeout);
            }
            else
            {
                while (key != ConsoleKey.Escape)
                {
                    switch (key)
                    {
                        case ConsoleKey.D1:
                            SendTransaction();
                            break;

                        case ConsoleKey.D2:
                            SendTransactionBlock();
                            break;
                    }

                    if (key != ConsoleKey.D3)
                        key = Console.ReadKey().Key;
                }
            }

            Console.WriteLine("Press any key to terminate the process...");
            Console.ReadKey(true);

            if (null != t1) t1.Dispose();
            if (null != t2) t2.Dispose();

            //Dispose connections
            foreach (PeerNode p in AppConfig.network)
            {
                if(null!=p.client)
                    p.client.Dispose();
            }

            s.Close();
        }

        /// <summary>
        /// Sends generated transaction to remote peers
        /// </summary>
        static void SendTransaction()
        {
            Transaction t = new Transaction { nodeId = AppConfig.Node.Id, seqNum = AppConfig.Node.seqNum++, timeStamp = DateTime.Now};

            using (MemoryStream ms = new MemoryStream())
            {
                DataContractJsonSerializer transactionSerializer = new DataContractJsonSerializer(typeof(Transaction));
                transactionSerializer.WriteObject(ms, t);

                byte[] data = ms.ToArray();

                string tranJson = Encoding.UTF8.GetString(data, 0, data.Length);
                Console.WriteLine($"Generated transaction: {tranJson}");

                foreach (PeerNode np in AppConfig.network)
                {
                    if (null == np.client) continue;
                    np.client.Send(new byte[] { 0x01 });
                    np.client.Send(data);
                    np.client.Send(new byte[] { 0x00 });
                }
            }
        }

        /// <summary>
        /// Sends a block of transactions to remote peers
        /// </summary>
        public static void SendTransactionBlock()
        {
            ReceivedTransaction[] transactions;

            lock (receivedTransactions)
            {
                transactions = receivedTransactions.ToArray();
                receivedTransactions.Clear();
            }

            using (MemoryStream ms = new MemoryStream())
            {
                TransactionBlock tb = new TransactionBlock { nodeId = AppConfig.Node.Id, timeStamp = DateTime.Now, transactions = transactions };
                DataContractJsonSerializer transactionSerializer = new DataContractJsonSerializer(typeof(TransactionBlock));
                transactionSerializer.WriteObject(ms, tb);

                byte[] data = ms.ToArray();
                string blockJson = Encoding.UTF8.GetString(data, 0, data.Length);
                Console.WriteLine($"Created block of transactions: {PrettifyBlockOfTransactions(blockJson)}");

                foreach (PeerNode np in AppConfig.network)
                {
                    if (null == np.client) continue;
                    np.client.Send(new byte[] { 0x02 });
                    np.client.Send(data);
                    np.client.Send(new byte[] { 0x00 });
                }
            }
        }

        /// <summary>
        /// Parses received data from remote peers
        /// </summary>
        /// <param name="buffer">Buffer that contains data</param>
        /// <param name="size">Size of the buffer</param>
        /// <param name="maxSize">Max allowed message size</param>
        /// <returns>Number of processed bytes from the buffer</returns>
        public static int ProcessMessages(EndPoint p, byte[] buffer, int size, int maxSize)
        {
            Console.WriteLine($"Received {size} bytes from {p}");

            int offset = 0;
            int msgSize = -1;
            while (offset < size && (msgSize = Array.IndexOf<byte>(buffer, 0, offset, size - offset)) >= 0)
            {
                if (msgSize > maxSize)
                {
                    Console.WriteLine("Message {0} bytes was discarded", msgSize - offset);
                }
                else
                {
                    Console.WriteLine("Message {0} bytes", msgSize - offset);

                    string bodyJson = Encoding.UTF8.GetString(buffer, offset + 1, msgSize - offset-1);

                    using (MemoryStream reader = new MemoryStream(buffer, offset + 1, msgSize - offset-1))
                    {
                        switch (buffer[offset])
                        {
                            case 0x01://Transaction
                                {
                                    Console.WriteLine("Transaction");

                                    DataContractJsonSerializer transactionSerializer = new DataContractJsonSerializer(typeof(Transaction));

                                    Transaction t = transactionSerializer.ReadObject(reader) as Transaction;

                                    ReceivedTransaction rt = new ReceivedTransaction { id = AppConfig.Node.Id, transaction = t, timeStamp = DateTime.Now };

                                    lock (receivedTransactions)
                                    {
                                        foreach (ReceivedTransaction it in receivedTransactions)
                                        {
                                            if (it.transaction.nodeId == rt.transaction.nodeId && it.transaction.seqNum == rt.transaction.seqNum)
                                            {
                                                it.count++;
                                                goto default;
                                            }
                                        }

                                        receivedTransactions.Add(rt);

                                        if (rt.transaction.nodeId != AppConfig.Node.Id)
                                        {
                                            //It is a new transaction so redirect it to all peers
                                            foreach (PeerNode np in AppConfig.network)
                                            {
                                                if (null == np.client) continue;
                                                if (rt.transaction.nodeId == np.Id) continue;

                                                np.client.Send(new byte[] { 0x01 });
                                                np.client.Send(buffer, offset + 1, msgSize - offset - 1);
                                                np.client.Send(new byte[] { 0x00 });
                                            }
                                        }
                                    }
                                }
                                break;

                            case 0x02://Block
                                {
                                    Console.WriteLine("Block");

                                    DataContractJsonSerializer blockSerializer = new DataContractJsonSerializer(typeof(TransactionBlock));

                                    TransactionBlock tb = blockSerializer.ReadObject(reader) as TransactionBlock;

                                    ReceivedTransactionBlock rt = new ReceivedTransactionBlock { transactionBlock = tb, timeStamp = DateTime.Now };

                                    string fileName = tb.nodeId + "_" + tb.timeStamp.ToString().Replace(":", "") + ".txt";
                                    using (FileStream fs = new FileStream(fileName, FileMode.Create, FileAccess.Write))
                                    {
                                        DataContractJsonSerializer rcvdBlockSerializer = new DataContractJsonSerializer(typeof(ReceivedTransactionBlock));
                                        rcvdBlockSerializer.WriteObject(fs, rt);
                                        fs.SetLength(fs.Position);
                                    }

                                    if (null != AppConfig.prevNode && tb.nodeId == AppConfig.prevNode.Id)
                                    {
                                        SendTransactionBlock();
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                    }
                    Console.WriteLine(PrettifyBlockOfTransactions(bodyJson));
                }

                offset = msgSize + 1;
            }

            return offset;
        }

        static string PrettifyBlockOfTransactions(string jsonBlock)
        {
            return jsonBlock.Replace("},{", "},\r\n{").Replace(",\"transactions\":[{", ",\r\n\"transactions\":[\r\n{").Replace("}]}", "}\r\n]\r\n}");
        }
    }
}
