using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Exchange;
using Microsoft.Azure.KeyVault.Models;
using System.Text;
using Microsoft.Azure.KeyVault;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.KeyVault.WebKey;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;
using System.Security.Cryptography.X509Certificates;
using Trading;
using RestSharp;
using System.Security.Cryptography;
using Microsoft.ApplicationInsights;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;

namespace Stateless1
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class Stateless1 : StatelessService

    {
        public Stateless1(StatelessServiceContext context)
            : base(context)
        { }
       

        //protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        //{

        //    return new[] { new ServiceInstanceListener(context =>this.CreateServiceRemotingListener(context)) };
        //}



        // Task<string> CreateKV.CreateKey(KeyBundle keyBundle, string keyName)
        //{
        //     KeyVaultClient keyVaultClient=null;
        //     InputValidator inputValidator=null;


        //   // inputValidator = new InputValidator(args);

        //    ServiceClientTracing.AddTracingInterceptor(new ConsoleTracingInterceptor());
        //    ServiceClientTracing.IsEnabled = inputValidator.GetTracingEnabled();

        //    var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
        //    var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

        //    var certificate = FindCertificateByThumbprint(cerificateThumbprint);
        //    var assertionCert = new ClientAssertionCertificate(clientId, certificate);

        //    keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
        //           (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
        //           new InjectHostHeaderHttpMessageHandler());

        //    // SECURITY: DO NOT USE IN PRODUCTION CODE; FOR TEST PURPOSES ONLY
        //    //  ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;

        //    List<KeyOperationType> successfulOperations = new List<KeyOperationType>();
        //    List<KeyOperationType> failedOperations = new List<KeyOperationType>();

        //    keyBundle = keyBundle ?? inputValidator.GetKeyBundle();
        //    var vaultAddress = inputValidator.GetVaultAddress();
        //   // keyName = inputValidator.GetKeyName();

        //    var tags = inputValidator.GetTags();

        //    var name = keyName;
        //    // Create key in the KeyVault key vault
        //    var createdKey = Task.Run(() =>
        //            keyVaultClient.CreateKeyAsync(vaultAddress, name, keyBundle.Key.Kty, keyAttributes: keyBundle.Attributes, tags: tags))
        //        .ConfigureAwait(false).GetAwaiter().GetResult();

        //    Console.Out.WriteLine("Created key:---------------");

        //    // Store the created key for the next operation if we have a sequence of operations
        //    return  null;
        //}
        public static async Task<string> GetAccessToken(string authority, string resource, string scope, ClientAssertionCertificate assertionCert)
        {
            var context = new AuthenticationContext(authority, TokenCache.DefaultShared);
            var result = await context.AcquireTokenAsync(resource, assertionCert).ConfigureAwait(false);

            return result.AccessToken;
        }
        public static X509Certificate2 FindCertificateByThumbprint(string certificateThumbprint)
        {
            if (certificateThumbprint == null)
                throw new System.ArgumentNullException("certificateThumbprint");

            //foreach (StoreLocation storeLocation in (StoreLocation[])
            //    Enum.GetValues(typeof(StoreLocation)))
            //{
            //    foreach (StoreName storeName in (StoreName[])
            //        Enum.GetValues(typeof(StoreName)))
            //    {
            //X509Store store = new X509Store(storeName, storeLocation);
            X509Store store = new X509Store(StoreName.My, StoreLocation.LocalMachine);

            store.Open(OpenFlags.ReadOnly);

            X509Certificate2Collection col = store.Certificates.Find(X509FindType.FindByThumbprint, certificateThumbprint, false); // Don't validate certs, since the test root isn't installed.

            if (col != null && col.Count != 0)
            {
                foreach (X509Certificate2 cert in col)
                {
                    if (cert.HasPrivateKey)
                    {
                        store.Close();
                        return cert;
                    }
                }
            }
            //  }
            //  }
            throw new System.Exception(
                    string.Format("Could not find the certificate with thumbprint {0} in any certificate store.", certificateThumbprint));
        }
        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            //long iterations = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

               // ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }

        public Task<string> CreateKey(KeyBundle keyBundle, string keyName)
        {
            KeyVaultClient keyVaultClient = null;
            InputValidator inputValidator = null;


            // inputValidator = new InputValidator(args);

            ServiceClientTracing.AddTracingInterceptor(new ConsoleTracingInterceptor());
            ServiceClientTracing.IsEnabled = inputValidator.GetTracingEnabled();

            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());

            // SECURITY: DO NOT USE IN PRODUCTION CODE; FOR TEST PURPOSES ONLY
            //  ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;

            List<KeyOperationType> successfulOperations = new List<KeyOperationType>();
            List<KeyOperationType> failedOperations = new List<KeyOperationType>();

            keyBundle = keyBundle ?? inputValidator.GetKeyBundle();
            var vaultAddress = inputValidator.GetVaultAddress();
            // keyName = inputValidator.GetKeyName();

            var tags = inputValidator.GetTags();

            var name = keyName;
            // Create key in the KeyVault key vault
            var createdKey = Task.Run(() =>
                    keyVaultClient.CreateKeyAsync(vaultAddress, name, keyBundle.Key.Kty, keyAttributes: keyBundle.Attributes, tags: tags))
                .ConfigureAwait(false).GetAwaiter().GetResult();

            Console.Out.WriteLine("Created key:---------------");

            // Store the created key for the next operation if we have a sequence of operations
            return null;
        }
    }
}
