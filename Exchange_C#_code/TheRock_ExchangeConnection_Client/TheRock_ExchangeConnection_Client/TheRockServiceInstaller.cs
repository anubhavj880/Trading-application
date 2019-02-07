using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace TheRock_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    public class TheRockServiceInstaller : Installer
    {
        public TheRockServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = TheRockMain.ServiceName;
            si.ServiceName = TheRockMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
