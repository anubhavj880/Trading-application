using System.ServiceProcess;

namespace TheRock_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = TheRockMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            TheRockMain.Start(args);
        }

        protected override void OnStop()
        {
            TheRockMain.Stop();
        }
    }
}
