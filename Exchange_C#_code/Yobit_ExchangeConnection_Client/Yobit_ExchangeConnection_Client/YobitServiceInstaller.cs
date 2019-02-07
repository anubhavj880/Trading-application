using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Yobit_ExchangeConnection_Client
{
    [RunInstallerAttribute(true)]
    public class YobitServiceInstaller : Installer
    {
        public YobitServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = YobitMain.ServiceName;
            si.ServiceName = YobitMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
