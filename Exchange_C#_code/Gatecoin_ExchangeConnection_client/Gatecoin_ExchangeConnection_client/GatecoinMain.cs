using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Gatecoin_ExchangeConnection_client
{
    class GatecoinMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Gatecoin_ExchangeConnection_client";
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
            log.Info("Gatecoin_ExchangeConnection_client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                        {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.gatecoin.com/Public/LiveTickers", "Gatecoin-ETHBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.gatecoin.com/Public/MarketDepth/ETHBTC", "Gatecoin-ETHBTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.gatecoin.com/Public/Transactions/ETHBTC", "Gatecoin-ETHBTC-Trade")),
                };
            Task.WaitAll(tasks);

        }
    }
}
