using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using GetKVKey;
using Microsoft.Azure.KeyVault.Models;
using Microsoft.Azure.KeyVault;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Security.Cryptography.X509Certificates;
using System.Security.Cryptography;
using Microsoft.ApplicationInsights;
using System.Text;
using RestSharp;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;

namespace CoinMoveMent
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class CoinMoveMent : StatefulService, GetKeyName
    {
        public CoinMoveMent(StatefulServiceContext context)
            : base(context)
        { }
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {

            return new[] { new ServiceReplicaListener(context => this.CreateServiceRemotingListener(context)) };
        }
        public Task<string> GetKey(string  appkey,string secretkey)
        {
            TelemetryClient tc = new TelemetryClient();

            string apiKey = appkey;
            Thread.Sleep(200);
            string secretKey = secretkey;

            var client = new RestClient("https://api.hitbtc.com");

            var request = new RestRequest("/api/1/trading/balance", Method.GET);
            request.AddParameter("nonce", GetNonce());
            request.AddParameter("apikey", apiKey);

            string sign = CalculateSignature(client.BuildUri(request).PathAndQuery, secretKey);
            request.AddHeader("X-Signature", sign);

            var response = client.Execute(request);
            //Console.WriteLine(response.Content);
            tc.TrackEvent("===> Response  for stock  Current Balance: " + response.Content);
            tc.Flush();

            return null;

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
       

       
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await myDictionary.TryGetValueAsync(tx, "Counter");

                    ServiceEventSource.Current.ServiceMessage(this.Context, "Current Counter Value: {0}",
                        result.HasValue ? result.Value.ToString() : "Value does not exist.");

                    await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);

                    // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                    // discarded, and nothing is saved to the secondary replicas.
                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
