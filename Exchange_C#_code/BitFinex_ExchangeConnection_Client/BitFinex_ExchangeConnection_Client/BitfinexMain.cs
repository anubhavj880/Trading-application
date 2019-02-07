using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace BitFinex_ExchangeConnection_Client
{
    class BitfinexMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "BitFinex_ExchangeConnection_Client";
        static void Main(string[] args)
        {
            if (Environment.UserInteractive)
            {
                // running as console app
                Start(args);

                Console.WriteLine("Press any key to stop...");
                Console.ReadKey(true);

                Stop();
            }
            else
            {
                // running as service
                using (var service = new Service())
                {
                    ServiceBase.Run(service);
                }
            }

        }

        public static void Stop()
        {
            log.Info("Bitfinex Service Stopped................");
        }

    
        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
            {
                 Task.Factory.StartNew(() => new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"book\",\"pair\": \"ETHBTC\",\"prec\": \"R0\",\"len\": \"100\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-ETHBTC-Order")),
                 /*Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"book\",\"pair\": \"LTCBTC\",\"prec\": \"R0\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-LTCBTC-Order")),
                 Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"book\",\"pair\": \"DSHBTC\",\"prec\": \"R0\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-DSHBTC-Order")),
                 Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"book\",\"pair\": \"XMRBTC\",\"prec\": \"R0\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-XMRBTC-Order")),
                 */Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"trades\",\"pair\": \"ETHBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-ETHBTC-Trade")),
                 /*Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"trades\",\"pair\": \"LTCBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-LTCBTC-Trade")),
                 Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"trades\",\"pair\": \"DSHBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-DSHBTC-Trade")),
                 Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"trades\",\"pair\": \"XMRBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-XMRBTC-Trade")),
                 */Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"ticker\",\"pair\": \"ETHBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-ETHBTC-Ticker")),
                 /*Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"ticker\",\"pair\": \"LTCBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-LTCBTC-Ticker")),
                 Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"ticker\",\"pair\": \"DSHBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-DSHBTC-Ticker")),
                 Task.Factory.StartNew(() =>  new Util().webSocketConnect("{\"event\": \"subscribe\",\"channel\": \"ticker\",\"pair\": \"XMRBTC\"}", "wss://api2.bitfinex.com:3000/ws", "BitFinex-XMRBTC-Ticker"))
                 */
                };
            Task.WaitAll(tasks);
         
        }
    }
}
