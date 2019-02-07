using System.ComponentModel;
using System.Configuration.Install;
using System.ServiceProcess;

namespace Gemini_ExchangeClient
{
    [RunInstallerAttribute(true)]
    public class GeminiServiceInstaller : Installer
    {
        public GeminiServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = GeminiMain.ServiceName;
            si.ServiceName = GeminiMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
