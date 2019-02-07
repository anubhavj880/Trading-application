using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Livecoin_ExchangeConnection_Client
{
    class LivecoinMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Livecoin_ExchangeConnection_Client";
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
            log.Info("Livecoin_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                 {
                 Task.Factory.StartNew(() => new Util().getOrderOrTicker(" https://api.livecoin.net/exchange/ticker", "LiveCoin-ETHBTC-LTCBTC-DSHBTC-XMRBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().getOrderOrTicker(" https://api.livecoin.net/exchange/all/order_book", "LiveCoin-ETHBTC-LTCBTC-DSHBTC-XMRBTC-Order")),
                 Task.Factory.StartNew(() => new Util().getTrade(" https://api.livecoin.net/exchange/last_trades?currencyPair=ETH/BTC&minutesOrHour=true", "LiveCoin-ETHBTC-Trade")),
                /*Task.Factory.StartNew(() => new Util().getTrade(" https://api.livecoin.net/exchange/last_trades?currencyPair=LTC/BTC&minutesOrHour=true", "LiveCoin-LTCBTC-Trade")),
                Task.Factory.StartNew(() => new Util().getTrade(" https://api.livecoin.net/exchange/last_trades?currencyPair=DASH/BTC&minutesOrHour=true", "LiveCoin-DSHBTC-Trade")),
                Task.Factory.StartNew(() => new Util().getTrade(" https://api.livecoin.net/exchange/last_trades?currencyPair=XMR/BTC&minutesOrHour=true", "LiveCoin-XMRBTC-Trade")),
                */
               };
            Task.WaitAll(tasks);
            Console.ReadLine();
        }
    }
}
