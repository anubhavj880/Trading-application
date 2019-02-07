using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Livecoin_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = LivecoinMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            LivecoinMain.Start(args);
        }

        protected override void OnStop()
        {
            LivecoinMain.Stop();
        }
    }
}
