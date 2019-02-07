using System;
using System.Threading.Tasks;
using System.ServiceProcess;

namespace Gemini_ExchangeClient
{
    class GeminiMain
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public const string ServiceName = "Gemini_ExchangeClient";
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
            log.Info("Gemini Service Stopped................");
        }


        public static void Start(string[] args)
        {
            Task[] tasks = new Task[]
            {
                 Task.Factory.StartNew(() => new Util().webSocketConnect( "wss://api.gemini.com/v1/marketdata/ETHBTC", "Gemini-ETHBTC-Order")),
                 Task.Factory.StartNew(() => new Util().SendHttpRequest( "https://api.gemini.com/v1/pubticker/ethbtc", "Gemini-ETHBTC-Ticker"))


                };
            Task.WaitAll(tasks);

        }
    }
}
