using System.ServiceProcess;

namespace Coinbase_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = CoinbaseMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            CoinbaseMain.Start(args);
        }

        protected override void OnStop()
        {
            CoinbaseMain.Stop();
        }
    }
}
