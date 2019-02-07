using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Liqui_ExchangeConnection_Client
{
    class LiquiMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Liqui_ExchangeConnection_Client";
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
            log.Info("Liqui_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                        {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/ticker/eth_btc", "Liqui-ETH_BTC-Ticker")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/ticker/ltc_btc", "Liqui-LTC_BTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/ticker/dash_btc", "Liqui-DASH_BTC-Ticker")),
                 */Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/depth/eth_btc", "Liqui-ETH_BTC-Order")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/depth/ltc_btc", "Liqui-LTC_BTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/depth/dash_btc", "Liqui-DASH_BTC-Order")),
                 */Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/trades/eth_btc", "Liqui-ETH_BTC-Trade")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/trades/ltc_btc", "Liqui-LTC_BTC-Trade")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.liqui.io/api/3/trades/dash_btc", "Liqui-DASH_BTC-Trade")),
                 */
                        };
            Task.WaitAll(tasks);
            
        }
    }
}
