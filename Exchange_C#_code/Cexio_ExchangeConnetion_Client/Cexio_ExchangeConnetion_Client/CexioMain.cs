using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Cexio_ExchangeConnetion_Client
{
    class CexioMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Cexio_ExchangeConnetion_Client";
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
            log.Info("Cexio_ExchangeConnetion_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
              {
                  Task.Factory.StartNew(() => new Util().SendHttpRequest("https://cex.io/api/ticker/ETH/BTC", "Cex_io-ETHBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://cex.io/api/order_book/ETH/BTC", "Cex_io-ETHBTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://cex.io/api/trade_history/ETH/BTC/", "Cex_io-ETHBTC-Trade")),
                 
              };
            Task.WaitAll(tasks);
          
        }
    }
}
