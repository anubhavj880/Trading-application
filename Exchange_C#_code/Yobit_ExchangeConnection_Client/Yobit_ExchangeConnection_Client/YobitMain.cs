using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Yobit_ExchangeConnection_Client
{
    class YobitMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Yobit_ExchangeConnection_Client";
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
            log.Info("Yobit_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                        {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/ticker/eth_btc", "Yobit-ETH_BTC-Ticker")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/ticker/ltc_btc", "Yobit-LTC_BTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/ticker/dash_btc", "Yobit-DASH_BTC-Ticker")),
                 */Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/depth/eth_btc", "Yobit-ETH_BTC-Order")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/depth/ltc_btc", "Yobit-LTC_BTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/depth/dash_btc", "Yobit-DASH_BTC-Order")),
                 */Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/trades/eth_btc", "Yobit-ETH_BTC-Trade")),
                /* Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/trades/ltc_btc", "Yobit-LTC_BTC-Trade")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest(" https://yobit.net/api/3/trades/dash_btc", "Yobit-DASH_BTC-Trade")),
                        */};
            Task.WaitAll(tasks);
           
        }
    }
}
