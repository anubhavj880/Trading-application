using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Exchange;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using Microsoft.Azure.KeyVault;
using Microsoft.Rest;

using System.Text;

using Microsoft.Azure.KeyVault.WebKey;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Serialization;
using System.Security.Cryptography.X509Certificates;
using RestSharp;
using System.Security.Cryptography;
using Microsoft.ApplicationInsights;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Trading;
using System.IO;
using GetKVKey;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.ServiceFabric.Services.Client;

namespace Stateful1
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class Stateful1 : StatefulService, CreateKV
    {
        public Stateful1(StatefulServiceContext context)
            : base(context)
        { }
 
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {

            return new[] { new ServiceReplicaListener(context => this.CreateServiceRemotingListener(context)) };
        }
        public Task<string> CreateKey(KeyBundle keyBundle, string appkey,string appsecret,string keyName,string[] args)
        {


            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator(args);
            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            keyBundle =  inputValidator.GetKeyBundle();
            var vaultAddress = inputValidator.GetVaultAddress();

            var tags = inputValidator.GetTags();

            var name = keyName;
            // Create key in the KeyVault key vault
            var createdKey = Task.Run(() =>
                    keyVaultClient.CreateKeyAsync(vaultAddress, name, keyBundle.Key.Kty, keyAttributes: keyBundle.Attributes, tags: tags))
                .ConfigureAwait(false).GetAwaiter().GetResult();


            // Store the created key for the next operation if we have a sequence of operations
            keyBundle= createdKey;



            // SECURITY: DO NOT USE IN PRODUCTION CODE; FOR TEST PURPOSES ONLY
            //  ServicePointManager.ServerCertificateValidationCallback += (sender, cert, chain, sslPolicyErrors) => true;
           
            Encrypt_ApiKey(keyBundle, appkey, keyName,args);
            EncryptSecret(keyBundle, appsecret, keyName);
           
             Decrypt(keyBundle);
            //GetKeyName kvKeyname = ServiceProxy.Create<GetKeyName>(new Uri(uriString: "fabric:/CoinMoveMentApp/CoinMoveMent"), new ServicePartitionKey(0));
            //kvKeyname.GetKey(keyBundle, keyName, cipherText, cipherText1);


            return null;
        }

        public Task<string> Encrypt_ApiKey(KeyBundle key,string appkey,string keyname,string[] args)
        {
            KeyOperationResult operationResult;
            InputValidator inputValidator = new InputValidator(args);

            var algorithm = inputValidator.GetEncryptionAlgorithm();
            byte[] plainText = System.Text.Encoding.UTF8.GetBytes(appkey);

            string keyVersion = inputValidator.GetKeyVersion();

            operationResult = _encrypt(key, keyVersion, algorithm, plainText, keyname, args);

            File.WriteAllText("cipherText.txt", Convert.ToBase64String(operationResult.Result));

            Console.Out.WriteLine(string.Format("The text is encrypted using key id {0} and algorithm {1}", operationResult.Kid, algorithm));
            Console.Out.WriteLine(string.Format("Encrypted text, base-64 encoded: {0}", Convert.ToBase64String(operationResult.Result)));
            string secretName = "apikey";
            CreateApiKey(out secretName, Convert.ToBase64String(operationResult.Result), keyname+"-appKey");
            return null;
        }
        private static SecretBundle CreateApiKey(out string secretName, string value,string secretapp)
        {
            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator();


            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            secretName = secretapp;
            // string secretValue = inputValidator.GetSecretValue();
            string secretValue = value;
            var tags = inputValidator.GetTags();

            var contentType = inputValidator.GetSecretContentType();

            var name = secretName;
            var secret = Task.Run(() =>
                    keyVaultClient.SetSecretAsync(inputValidator.GetVaultAddress(), name, secretValue, tags, contentType, inputValidator.GetSecretAttributes()))
                .ConfigureAwait(false).GetAwaiter().GetResult();

            Console.Out.WriteLine("Created/Updated secret:---------------");

            return secret;
        }
        private static void EncryptSecret(KeyBundle key,string appsecret,string keyname)
        {
            KeyOperationResult operationResult;
            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator();


            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            var algorithm = inputValidator.GetEncryptionAlgorithm();
            byte[] plainText = System.Text.Encoding.UTF8.GetBytes(appsecret);

            string keyVersion = inputValidator.GetKeyVersion();

            operationResult = _encryptSecret(key, keyVersion, algorithm, plainText);

            File.WriteAllText("cipherText1.txt", Convert.ToBase64String(operationResult.Result));

            Console.Out.WriteLine(string.Format("The text is encrypted using key id {0} and algorithm {1}", operationResult.Kid, algorithm));
            Console.Out.WriteLine(string.Format("Encrypted text, base-64 encoded: {0}", Convert.ToBase64String(operationResult.Result)));
            string secretName = "secret";
            CreateSecret(secretName, Convert.ToBase64String(operationResult.Result), keyname + "-appSecret");
        }
        private static SecretBundle CreateSecret(string secretName, string value,string keyname)
        {
            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator();

            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            // string secretValue = inputValidator.GetSecretValue();
            string secretValue = value;
            var tags = inputValidator.GetTags();

            var contentType = inputValidator.GetSecretContentType();

            var name = keyname;
            var secret = Task.Run(() =>
                    keyVaultClient.SetSecretAsync(inputValidator.GetVaultAddress(), name, secretValue, tags, contentType, inputValidator.GetSecretAttributes()))
                .ConfigureAwait(false).GetAwaiter().GetResult();

            Console.Out.WriteLine("Created/Updated secret:---------------");

            return secret;
        }
        private static KeyOperationResult _encryptSecret(KeyBundle key, string keyVersion, string algorithm, byte[] plainText)
        {
            KeyOperationResult operationResult;
            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator();


            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            if (keyVersion != string.Empty)
            {
                var vaultAddress = inputValidator.GetVaultAddress();
                string keyName = "tradsecretKey";

                // Encrypt the input data using the specified algorithm
                operationResult = Task.Run(() => keyVaultClient.EncryptAsync(vaultAddress, keyName, keyVersion, algorithm, plainText)).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            else
            {
                // If the key is not initialized get the key id from args
                var keyId = (key != null) ? key.Key.Kid : inputValidator.GetKeyId();
                // Encrypt the input data using the specified algorithm
                operationResult = Task.Run(() => keyVaultClient.EncryptAsync(keyId, algorithm, plainText)).ConfigureAwait(false).GetAwaiter().GetResult();
            }

            return operationResult;
        }
        private static void Decrypt(KeyBundle key)
        {
            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator();


            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            TelemetryClient tc = new TelemetryClient();
            KeyOperationResult operationResult;

            var algorithm = inputValidator.GetEncryptionAlgorithm();
            var cipherText = inputValidator.GetCipherText();
            var cipherText1 = inputValidator.GetCipherText1();

            KeyBundle localKey;

            localKey = (key ?? GetKey(null));

            // Decrypt the encrypted data
            operationResult = Task.Run(() => keyVaultClient.DecryptAsync(localKey.KeyIdentifier.ToString(), algorithm, cipherText)).ConfigureAwait(false).GetAwaiter().GetResult();
            Thread.Sleep(200);
            string apiKey = Encoding.UTF8.GetString(operationResult.Result);
            operationResult = Task.Run(() => keyVaultClient.DecryptAsync(localKey.KeyIdentifier.ToString(), algorithm, cipherText1)).ConfigureAwait(false).GetAwaiter().GetResult();
            Thread.Sleep(200);
            string secretKey = Encoding.UTF8.GetString(operationResult.Result);

          GetKeyName kvKeyname = ServiceProxy.Create<GetKeyName>(new Uri(uriString: "fabric:/CoinMoveMentApp/CoinMoveMent"), new ServicePartitionKey(0));
            kvKeyname.GetKey(apiKey, secretKey);


        }

        private static long GetNonce()
        {
            return DateTime.Now.Ticks * 10 / TimeSpan.TicksPerMillisecond; // use millisecond timestamp or whatever you want
        }

        public static string CalculateSignature(string text, string secretKey)
        {
            using (var hmacsha512 = new HMACSHA512(Encoding.UTF8.GetBytes(secretKey)))
            {
                hmacsha512.ComputeHash(Encoding.UTF8.GetBytes(text));
                return string.Concat(hmacsha512.Hash.Select(b => b.ToString("x2")).ToArray()); // minimalistic hex-encoding and lower case
            }
        }
        private static KeyBundle GetKey(KeyBundle key)
        {
            KeyVaultClient keyVaultClient = null;

            InputValidator inputValidator = new InputValidator();


            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);

            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                   (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                   new InjectHostHeaderHttpMessageHandler());
            KeyBundle retrievedKey;
            string keyVersion = inputValidator.GetKeyVersion();
            string keyName = inputValidator.GetKeyName(allowDefault: false);

            if (keyVersion != string.Empty || keyName != string.Empty)
            {
                var vaultAddress = inputValidator.GetVaultAddress();
                if (keyVersion != string.Empty)
                {
                    keyName = inputValidator.GetKeyName(true);
                    retrievedKey = Task.Run(() => keyVaultClient.GetKeyAsync(vaultAddress, keyName, keyVersion)).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                else
                {
                    retrievedKey = Task.Run(() => keyVaultClient.GetKeyAsync(vaultAddress, keyName)).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
            else
            {
                // If the key is not initialized get the key id from args
                var keyId = (key != null) ? key.Key.Kid : inputValidator.GetKeyId();

                // Get the key using its ID
                retrievedKey = Task.Run(() => keyVaultClient.GetKeyAsync(keyId)).ConfigureAwait(false).GetAwaiter().GetResult();
            }

            Console.Out.WriteLine("Retrived key:---------------");

            //store the created key for the next operation if we have a sequence of operations
            return retrievedKey;
        }
        private static KeyOperationResult _encrypt(KeyBundle key, string keyVersion, string algorithm, byte[] plainText,string keyname,string[] args)
        {
            KeyOperationResult operationResult;

           InputValidator inputValidator = new InputValidator(args);
            KeyVaultClient keyVaultClient = null;

            var clientId = "8272fc9d-cae5-4352-ac81-15531d538b79";
            var cerificateThumbprint = "718CE2AA500409A2B85DD230D97921E77FBC4641";

            var certificate = FindCertificateByThumbprint(cerificateThumbprint);
            var assertionCert = new ClientAssertionCertificate(clientId, certificate);
            keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(
                  (authority, resource, scope) => GetAccessToken(authority, resource, scope, assertionCert)),
                  new InjectHostHeaderHttpMessageHandler());
            if (keyVersion != string.Empty)
            {
                var vaultAddress = inputValidator.GetVaultAddress();
                string keyName = keyname;

                // Encrypt the input data using the specified algorithm
                operationResult = Task.Run(() => keyVaultClient.EncryptAsync(vaultAddress, keyName, keyVersion, algorithm, plainText)).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            else
            {
                // If the key is not initialized get the key id from args
                var keyId = (key != null) ? key.Key.Kid : inputValidator.GetKeyId();
                // Encrypt the input data using the specified algorithm
                operationResult = Task.Run(() => keyVaultClient.EncryptAsync(keyId, algorithm, plainText)).ConfigureAwait(false).GetAwaiter().GetResult();
            }

            return operationResult;
        }

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

        //public Task<string> GetKey(KeyBundle key, string keyname)
        //{
        //    throw new NotImplementedException();
        //}
    }
}
