using System.ServiceProcess;

namespace HitBtc
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = HitbtcMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            HitbtcMain.Start(args);
        }

        protected override void OnStop()
        {
            HitbtcMain.Stop();
        }
    }
}
