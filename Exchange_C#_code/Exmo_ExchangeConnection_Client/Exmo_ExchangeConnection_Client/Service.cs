using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Exmo_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = ExmoMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            ExmoMain.Start(args);
        }

        protected override void OnStop()
        {
            ExmoMain.Stop();
        }
    }
}
