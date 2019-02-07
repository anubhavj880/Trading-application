using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Kraken_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    public class KrakenServiceInstaller : Installer
    {
        public KrakenServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = KrakenMain.ServiceName;
            si.ServiceName = KrakenMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
