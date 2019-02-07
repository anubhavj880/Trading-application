using System.ServiceProcess;

namespace Kraken_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = KrakenMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            KrakenMain.Start(args);
        }

        protected override void OnStop()
        {
            KrakenMain.Stop();
        }
    }
}
