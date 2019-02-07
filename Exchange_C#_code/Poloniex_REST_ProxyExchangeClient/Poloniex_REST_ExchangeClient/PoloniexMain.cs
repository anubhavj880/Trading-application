using System;
using System.ServiceProcess;
using System.Threading.Tasks;

namespace Poloniex_REST_ExchangeClient
{
    class PoloniexMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Poloniex_REST_ExchangeClient";
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
            log.Info("Poloniex_ExchangeConnection_Client Service Stopped................");
        }

        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
              {
                 Task.Factory.StartNew(() => new Util().getPoloniexmarketData("http://uk2.my-proxy.com/index.php?q=09ja2tytk5WopqSmptTJ3pjM4tGVqKyao6HOo8nZ1uDF1Jx0qpys4NbUudvXydh6pqeiXs7Z2NzO4cffiJihqXWtuKnJrses", "Poloniex-BTC_ETH-Order")),
                 // Task.Factory.StartNew(() => new Util().getPoloniexmarketData("https://us7.proxysite.com/process.php?d=x5B99F6XEldcjs7HG1Tcg4tWInkMg2gtrcq6zSuFuK%2B0EnG1emcOTmlH7gI%3D&b=1&f=norefer", "Poloniex-BTC_ETH-Ticker")),
                  Task.Factory.StartNew(() => new Util().getPoloniexmarketData("http://uk2.my-proxy.com/index.php?q=09ja2tytk5WopqSmptTJ3pjM4tGVqKyao6HOo8nZ1uDF1Jx0qpys4NbUvtvUyMuAoKurp93djM3e5dbLppqxh5nU1qOsvbbDq4x_", "Poloniex-BTC_ETH-Trade")),

             };
            Task.WaitAll(tasks);

        }
    }
}
