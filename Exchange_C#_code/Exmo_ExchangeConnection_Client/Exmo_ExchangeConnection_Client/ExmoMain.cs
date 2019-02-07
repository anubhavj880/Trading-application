using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Exmo_ExchangeConnection_Client
{
    class ExmoMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Exmo_ExchangeConnection_Client";
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
            log.Info("Exmo_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                          {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.exmo.com/v1/ticker/", "Exmo-ETH_BTC-DASH_BTC-LTC_BTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.exmo.com/v1/order_book/?pair=ETH_BTC", "Exmo-ETH_BTC-Order")),
                 //Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.exmo.com/v1/order_book/?pair=DASH_BTC", "Exmo-DASH_BTC-Order")),
                // Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.exmo.com/v1/order_book/?pair=LTC_BTC", "Exmo-LTC_BTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.exmo.com/v1/trades/?pair=ETH_BTC,DASH_BTC,LTC_BTC", "Exmo-ETH_BTC-DASH_BTC-LTC_BTC-Trade")),
                          };
            Task.WaitAll(tasks);

        }
    }
}
