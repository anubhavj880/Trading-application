using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Exmo_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    public class ExmoServiceInstaller : Installer
    {
        public ExmoServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = ExmoMain.ServiceName;
            si.ServiceName = ExmoMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
