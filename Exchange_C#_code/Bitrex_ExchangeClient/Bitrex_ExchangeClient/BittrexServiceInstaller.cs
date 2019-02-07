﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration.Install;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace Bittrex_ExchangeClient
{
    [RunInstallerAttribute(true)]
    public class BittrexServiceInstaller : Installer
    {
        public BittrexServiceInstaller()
        {
            var spi = new ServiceProcessInstaller();
            var si = new ServiceInstaller();

            spi.Account = ServiceAccount.LocalSystem;
            spi.Username = null;
            spi.Password = null;

            si.DisplayName = BittrexMain.ServiceName;
            si.ServiceName = BittrexMain.ServiceName;
            si.StartType = ServiceStartMode.Automatic;

            Installers.Add(spi);
            Installers.Add(si);
        }
    }
}
