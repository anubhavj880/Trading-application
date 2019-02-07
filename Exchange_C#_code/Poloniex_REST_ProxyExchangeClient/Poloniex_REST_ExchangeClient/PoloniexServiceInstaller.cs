using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Poloniex_REST_ExchangeClient
{
    [RunInstallerAttribute(true)]
    public class PoloniexServiceInstaller : Installer
    {
        public PoloniexServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = PoloniexMain.ServiceName;
            si.ServiceName = PoloniexMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
