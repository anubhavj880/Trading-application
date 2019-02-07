using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Kraken_ExchangeConnection_Client
{
    class KrakenMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Kraken_ExchangeConnection_Client";
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
            log.Info("Kraken_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
                         {
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Ticker?pair=ETHXBT", "Kraken-XETHXXBT-Ticker")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Ticker?pair=LTCXBT", "Kraken-XLTCXXBT-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Ticker?pair=XMRXBT", "Kraken-XXMRXXBT-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Ticker?pair=DASHXBT"Ticker, "Kraken-DASHXBT-Ticker")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Ticker?pair=XRPXBT", "Kraken-XXRPXXBT-Ticker")),
                 */Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Depth?pair=ETHXBT", "Kraken-XETHXXBT-Order")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Depth?pair=LTCXBT", "Kraken-XLTCXXBT-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Depth?pair=XMRXBT", "Kraken-XXMRXXBT-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Depth?pair=DASHXBT", "Kraken-DASHXBT-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Depth?pair=XRPXBT", "Kraken-XXRPXXBT-Order")),
                 */Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Trades?pair=ETHXBT", "Kraken-XETHXXBT-Trade")),
                 /*Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Trades?pair=LTCXBT", "Kraken-XLTCXXBT-Trade")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Trades?pair=XMRXBT", "Kraken-XXMRXXBT-Trade")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Trades?pair=DASHXBT", "Kraken-DASHXBT-Trade")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest("https://api.kraken.com/0/public/Trades?pair=XRPXBT", "Kraken-XXRPXXBT-Trade")),
                 */
                         };
            Task.WaitAll(tasks);
           
        }
    }
}
