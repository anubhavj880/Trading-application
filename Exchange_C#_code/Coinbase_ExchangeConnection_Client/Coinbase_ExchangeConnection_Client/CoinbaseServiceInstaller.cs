using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Coinbase_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    public class CoinbaseServiceInstaller : Installer
    {
        public CoinbaseServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = CoinbaseMain.ServiceName;
            si.ServiceName = CoinbaseMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
