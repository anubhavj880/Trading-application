using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Gatecoin_ExchangeConnection_client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = GatecoinMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            GatecoinMain.Start(args);
        }

        protected override void OnStop()
        {
            GatecoinMain.Stop();
        }
    }
}
