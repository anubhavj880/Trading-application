using QuadrigaCX_ExchangeConnection_Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace QuardrigaCX_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    class QuadrigaCXServiceInstaller : Installer
    {
        public QuadrigaCXServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = QuadrigaCXMain.ServiceName;
            si.ServiceName = QuadrigaCXMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
