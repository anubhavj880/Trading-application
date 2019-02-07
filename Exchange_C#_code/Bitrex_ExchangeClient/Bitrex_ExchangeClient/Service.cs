using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Bittrex_ExchangeClient
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = BittrexMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            BittrexMain.Start(args);
        }

        protected override void OnStop()
        {
            BittrexMain.Stop();
        }
    }
}
