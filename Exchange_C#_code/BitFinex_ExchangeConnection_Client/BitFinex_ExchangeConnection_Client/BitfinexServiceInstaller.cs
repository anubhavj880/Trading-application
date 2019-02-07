using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace BitFinex_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    public class BitfinexServiceInstaller : Installer
    {
        public BitfinexServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = BitfinexMain.ServiceName;
            si.ServiceName = BitfinexMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
