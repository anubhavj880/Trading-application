using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Yobit_Coinmovement_Application
{
    class Yobit
    {
        private readonly string _url = "https://yobit.net/tapi";
        private RestClient GetRestClient()
        {
            var client = new RestClient();
            var url = _url;
            client.BaseUrl = new System.Uri(url);
            return client;
        }
        private RestRequest GetRestRequest(String method, long nonces, Dictionary<string, Object> param = null)
        {
            var url ="method="+ method + "&nonce=" + (int)nonces;
            if (param != null)
            {
                foreach (var item in param)
                {
                    url = url + "&" + item.Key + "=" + item.Value;
                }
            }
            Console.WriteLine(url);
            var request = new RestRequest();
            request.Method = Method.POST;
            request.AddHeader("Key", getApikey());
            request.AddHeader("Sign", GetHexHashSignature(url));

            return request;
        }
        private static string getApikey()
        {
            return "A6A005CE32F836C5E6C23F5C310F2FA8";
        }
        private static string getSecretkey()
        {
            return "1e22fd4cfd4e1cdd16ded680c9597a2b";
        }
        private string GetHexHashSignature(string payload)
        {
            HMACSHA512 hmac = new HMACSHA512(Encoding.UTF8.GetBytes(getSecretkey()));
            byte[] hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(payload));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
        private long GetNonce()
        {
            return DateTime.Now.Ticks * 10 / TimeSpan.TicksPerSecond;
        }
        private static IRestResponse GetRestResponse(RestClient client, RestRequest restRequest)
        {
            var response = client.Execute(restRequest);

            return response;
        }
        private IRestResponse QueryPrivate( String method, Dictionary<string, Object> param = null)
        {
            var nonces = GetNonce();
            var client = GetRestClient();
            var restRequest = GetRestRequest(method, nonces, param);
            restRequest.AddParameter("method", method);
            restRequest.AddParameter("nonce", (int)nonces);
            if (param != null)
            {
                foreach (var item in param)
                {
                    restRequest.AddParameter(item.Key, item.Value);
                }
            }
            var response = GetRestResponse(client, restRequest);
            return response;
        }

        /// <summary>
        /// Gets the account balance.
        /// </summary>
        /// <returns></returns>
        public IRestResponse GetAccountBalances()
        {
            return QueryPrivate("getInfo");

        }

        public IRestResponse withdraw( string coinName = null, string amount = null, string address = null)
        {
            var param = new Dictionary<string, Object>();
            if (coinName != null)
                param.Add("coinName", coinName);
            if (amount != null)
                param.Add("amount", Convert.ToDouble(amount));
            if (address != null)
                param.Add("address", address);

            return QueryPrivate("WithdrawCoinsToAddress", param);
        }
        public IRestResponse getDepositeAddress(string coinName = null, string need_new = null)
        {
            var param = new Dictionary<string, Object>();
            if (coinName != null)
                param.Add("coinName", coinName);
            if (need_new != null)
                param.Add("need_new", Convert.ToInt64(need_new));
            return QueryPrivate("GetDepositAddress", param);
        }
        
       


    }
}