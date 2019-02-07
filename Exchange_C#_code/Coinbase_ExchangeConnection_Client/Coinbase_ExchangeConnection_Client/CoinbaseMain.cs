using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Coinbase_ExchangeConnection_Client
{
    class CoinbaseMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Coinbase_ExchangeConnection_Client";
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
            log.Info("Coinbase_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
             {
                Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.gdax.com/products/ETH-BTC/ticker", "CoinBase-ETHBTC-Ticker")),
                //Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.gdax.com/products/LTC-BTC/ticker", "CoinBase-LTCBTC-Ticker")),
                Task.Factory.StartNew(() => new Util().webSocketConnect("{\"type\": \"subscribe\",\"product_ids\": [\"ETH-BTC\"]}", "wss://ws-feed.gdax.com", "ETHBTC","https://api.gdax.com/products/ETH-BTC/book?level=3")),
                 //Task.Factory.StartNew(() => new Util().webSocketConnect("{\"type\": \"subscribe\",\"product_ids\": [\"LTC-BTC\"]}", "wss://ws-feed.gdax.com", "LTCBTC","https://api.gdax.com/products/LTC-BTC/book?level=3"))

             };
            Task.WaitAll(tasks);

        }
    }
}
