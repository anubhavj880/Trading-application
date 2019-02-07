using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace HitBtc
{
    class HitbtcMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "HitBtc";
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
            log.Info("HitBtc Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
            {
                Task.Factory.StartNew(() => new Util().getTickerData("api/1/public/ETHBTC/ticker", "Hitbtc-ETHBTC-Ticker")),
               /* Task.Factory.StartNew(() => new Util().getTickerData("api/1/public/LTCBTC/ticker","Hitbtc-LTCBTC-Ticker")),
                Task.Factory.StartNew(() => new Util().getTickerData("api/1/public/XMRBTC/ticker","Hitbtc-XMRBTC-Ticker")),
                Task.Factory.StartNew(() => new Util().getTickerData("api/1/public/DASHBTC/ticker","Hitbtc-DASHBTC-Ticker")),
                */Task.Factory.StartNew(() => Util.getOrderAndTradeData("ws://api.hitbtc.com:80/","ETHBTC-LTCBTC-DASHBTC-XMRBTC")),
            };
            Task.WaitAll(tasks);
          
        }
    }
}
