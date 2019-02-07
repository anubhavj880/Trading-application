using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace TheRock_ExchangeConnection_Client
{
    class TheRockMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "TheRock_ExchangeConnection_Client";
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
            log.Info("TheRock_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
              {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.therocktrading.com/v1/funds/ETHBTC/ticker", "TheRock-ETHBTC-Ticker")),
                /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.therocktrading.com/v1/funds/LTCBTC/ticker", "TheRock-LTCBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.therocktrading.com/v1/funds/BTCXRP/ticker", "TheRock-BTCXRP-Ticker")),
               */ Task.Factory.StartNew(() => Util.Pusher("bb1fafdf79a00453b5af", "TheRock-ETHBTC-trade", "currency")),
                /*Task.Factory.StartNew(() => Util.Pusher("bb1fafdf79a00453b5af","TheRock-LTCBTC-trade", "currency")),
                Task.Factory.StartNew(() => Util.Pusher("bb1fafdf79a00453b5af", "TheRock-BTCXRP-trade", "currency")),
                 */Task.Factory.StartNew(() => Util.Pusher("bb1fafdf79a00453b5af", "TheRock-ETHBTC-order", "ETHBTC")),
                /*Task.Factory.StartNew(() => Util.Pusher("bb1fafdf79a00453b5af", "TheRock-LTCBTC-order", "LTCBTC")),
               Task.Factory.StartNew(() => Util.Pusher("bb1fafdf79a00453b5af", "TheRock-BTCXRP-order", "BTCXRP")),
               */
                 };
            Task.WaitAll(tasks);

        }
    }
}
