using System.ServiceProcess;

namespace Yobit_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = YobitMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            YobitMain.Start(args);
        }

        protected override void OnStop()
        {
            YobitMain.Stop();
        }
    }
}
