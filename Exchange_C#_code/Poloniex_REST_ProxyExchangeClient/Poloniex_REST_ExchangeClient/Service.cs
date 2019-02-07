using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Poloniex_REST_ExchangeClient
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = PoloniexMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            PoloniexMain.Start(args);
        }

        protected override void OnStop()
        {
            PoloniexMain.Stop();
        }
    }
}