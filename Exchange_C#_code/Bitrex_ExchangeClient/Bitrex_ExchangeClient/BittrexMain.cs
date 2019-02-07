using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Bittrex_ExchangeClient
{
    class BittrexMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Bittrex_ExchangeClient";
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
            log.Info("Bittrex_ExchangeClient Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                         {

                Task.Factory.StartNew(() => new Util().getMarketData("https://bittrex.com/api/v1.1/public/getorderbook?market=BTC-ETH&type=both&depth=50", "Bittrex-ETHBTC-Order")),
                 /*Task.Factory.StartNew(() => new Util().getMarketData("public/getorderbook?market=BTC-LTC&type=both&depth=50", "Bittrex-LTCBTC-Order")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getorderbook?market=BTC-DASH&type=both&depth=50", "Bittrex-DSHBTC-Order")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getorderbook?market=BTC-XMR&type=both&depth=50", "Bittrex-XMRBTC-Order")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getorderbook?market=BTC-XRP&type=both&depth=50", "Bittrex-XRPBTC-Order")),
                  */Task.Factory.StartNew(() => new Util().getMarketData("https://bittrex.com/api/v1.1/public/getticker?market=BTC-ETH", "Bittrex-ETHBTC-Ticker")),
                /*Task.Factory.StartNew(() => new Util().getMarketData("public/getticker?market=BTC-LTC", "Bittrex-LTCBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getticker?market=BTC-DASH", "Bittrex-DSHBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getticker?market=BTC-XMR", "Bittrex-XMRBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getticker?market=BTC-XRP", "Bittrex-XRPBTC-Ticker")),   
                 */Task.Factory.StartNew(() => new Util().getMarketData("https://bittrex.com/api/v1.1/public/getmarkethistory?market=BTC-ETH", "Bittrex-ETHBTC-Trade")),
                 /*Task.Factory.StartNew(() => new Util().getMarketData("public/getmarkethistory?market=BTC-LTC", "Bittrex-LTCBTC-Trade")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getmarkethistory?market=BTC-DASH", "Bittrex-DSHBTC-Trade")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getmarkethistory?market=BTC-XMR", "Bittrex-XMRBTC-Trade")),
                 Task.Factory.StartNew(() => new Util().getMarketData("public/getmarkethistory?market=BTC-XRP", "Bittrex-XRPBTC-Trade")),
                 */
                         };
            Task.WaitAll(tasks);

        }
    }
}
