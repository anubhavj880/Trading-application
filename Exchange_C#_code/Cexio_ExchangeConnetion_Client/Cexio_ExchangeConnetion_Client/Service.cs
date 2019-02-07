using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Cexio_ExchangeConnetion_Client
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = CexioMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            CexioMain.Start(args);
        }

        protected override void OnStop()
        {
            CexioMain.Stop();
        }
    }
}
