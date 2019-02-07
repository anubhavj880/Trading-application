using System.ServiceProcess;

namespace Liqui_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = LiquiMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            LiquiMain.Start(args);
        }

        protected override void OnStop()
        {
            LiquiMain.Stop();
        }
    }
}
