using QuardrigaCX_ExchangeConnection_Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace QuadrigaCX_ExchangeConnection_Client
{

    public class QuadrigaCXMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "QuadrigaCX_ExchangeConnection_Client";
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
            log.Info("QuadrigaCX_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                        {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.quadrigacx.com/v2/ticker?book=eth_btc", "QuadrigaCX-ETHBTC-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.quadrigacx.com/v2/order_book?book=eth_btc", "QuadrigaCX-ETHBTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.quadrigacx.com/v2/transactions?book=eth_btc", "QuadrigaCX-ETHBTC-Trade")),
                };
            Task.WaitAll(tasks);

        }
    }
}
