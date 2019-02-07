using QuadrigaCX_ExchangeConnection_Client;
using System.ServiceProcess;

namespace QuardrigaCX_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = QuadrigaCXMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            QuadrigaCXMain.Start(args);
        }

        protected override void OnStop()
        {
            QuadrigaCXMain.Stop();
        }
    }
}

