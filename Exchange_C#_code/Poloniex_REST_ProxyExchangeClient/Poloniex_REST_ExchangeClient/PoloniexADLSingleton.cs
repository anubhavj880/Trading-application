using Microsoft.Azure.Management.DataLake.Store;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Poloniex_REST_ExchangeClient
{
    public sealed class PoloniexADLSingleton
    {
        private static volatile PoloniexADLSingleton instance;
        private static object syncRoot = new Object();

        private PoloniexADLSingleton() { }

        public static PoloniexADLSingleton Instance
        {
            get
            {
                if (instance == null)
                {
                    lock (syncRoot)
                    {
                        if (instance == null)
                            instance = new PoloniexADLSingleton();
                    }
                }

                return instance;
            }
        }
        public async Task<DataLakeStoreFileSystemManagementClient> getADLaccess()
        {
            
            string _subId = "63c86017-0151-44cb-9107-ed0bd2476414";
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());
            var domain = "https://login.windows.net/cac8ed95-253c-4ea2-9f59-0f809f87957a/oauth2/token";
            var webApp_clientId = "f26b6f5e-c581-41ba-a835-96d3325a4ab2";
            var clientSecret = "bFn1nVqdaVnuTtSbHwkeh5vzXzWMTrTEC6+3Lfb5ROQ=";
            var clientCredential = new ClientCredential(webApp_clientId, clientSecret);
            var creds = await ApplicationTokenProvider.LoginSilentAsync(domain, clientCredential);
            DataLakeStoreAccountManagementClient _adlsClient = new DataLakeStoreAccountManagementClient(creds) { SubscriptionId = _subId };
            DataLakeStoreFileSystemManagementClient _adlsFileSystemClient = new DataLakeStoreFileSystemManagementClient(creds);
            return _adlsFileSystemClient;
        }
    }
}
