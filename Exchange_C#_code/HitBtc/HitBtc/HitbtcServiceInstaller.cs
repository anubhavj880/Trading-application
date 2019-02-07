using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace HitBtc
{
    [RunInstallerAttribute(true)]
    public class HitbtcServiceInstaller : Installer
    {
        public HitbtcServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = HitbtcMain.ServiceName;
            si.ServiceName = HitbtcMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
