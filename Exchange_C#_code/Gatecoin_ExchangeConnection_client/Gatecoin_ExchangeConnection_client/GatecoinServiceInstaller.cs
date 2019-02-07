using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Gatecoin_ExchangeConnection_client
{
    [RunInstallerAttribute(true)]
    class GatecoinServiceInstaller : Installer
    {
        public GatecoinServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = GatecoinMain.ServiceName;
            si.ServiceName = GatecoinMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
