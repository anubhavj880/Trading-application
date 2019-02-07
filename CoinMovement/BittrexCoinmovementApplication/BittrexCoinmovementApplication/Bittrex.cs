using RestSharp;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace BittrexCoinmovementApplication
{
    class Bittrex
    {
        private readonly string _url = "https://bittrex.com/api/v1.1/account/";
        private RestClient GetRestClient(string requestUrl)
        {
            var client = new RestClient();
            var url = _url + requestUrl;
            client.BaseUrl = new System.Uri(url);
            return client;
        }
        private RestRequest GetRestRequest(String requestUrl, long nonces, Dictionary<string, Object> param = null)
        {
            var url = _url + requestUrl + "?apikey=" + getApikey() + "&nonce=" + nonces;
            if (param != null)
            {
                foreach (var item in param)
                {
                    url = url + "&" + item.Key + "=" + item.Value;
                }
            }
            Console.WriteLine(url);
            var request = new RestRequest();
            request.Method = Method.GET;
            request.AddHeader("apisign", GetHexHashSignature(url));

            return request;
        }
        private static string getApikey()
        {
            return "7955c7e2b5c243959ca47f4b3efa59d9";
        }
        private static string getSecretkey()
        {
            return "bb7cf599314c47a1942e05d1e29eb304";
        }
        private string GetHexHashSignature(string payload)
        {
            HMACSHA512 hmac = new HMACSHA512(Encoding.UTF8.GetBytes(getSecretkey()));
            byte[] hash = hmac.ComputeHash(Encoding.UTF8.GetBytes(payload));
            return BitConverter.ToString(hash).Replace("-", "").ToLower();
        }
        private long GetNonce()
        {
            return DateTime.Now.Ticks * 10 / TimeSpan.TicksPerMillisecond;
        }
        private static IRestResponse GetRestResponse(RestClient client, RestRequest restRequest)
        {
            var response = client.Execute(restRequest);

            return response;
        }
        private IRestResponse QueryPrivate(string requestUrl, Dictionary<string, Object> param = null)
        {
            var nonces = GetNonce();
            var client = GetRestClient(requestUrl);
            var restRequest = GetRestRequest(requestUrl, nonces, param);
            restRequest.AddParameter("apikey", getApikey());
            restRequest.AddParameter("nonce", nonces);
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
            return QueryPrivate("getbalances");

        }
        public IRestResponse GetAccountBalance(String currency)
        {
            var param = new Dictionary<string, Object>();
            param.Add("currency", currency);
            return QueryPrivate("getbalance", param);

        }

        public IRestResponse withdraw(string currency = null, string quantity = null, string address = null, string paymentid = null)
        {
            var param = new Dictionary<string, Object>();
            if (currency != null)
                param.Add("currency", currency);
            if (quantity != null)
                param.Add("quantity", Convert.ToDouble(quantity));
            if (paymentid != null)
                param.Add("paymentid", paymentid);
            if (address != null)
                param.Add("address", address);
          
            return QueryPrivate("withdraw", param);
        }
        public IRestResponse getDepositeAddress(string currency = null)
        {
            var param = new Dictionary<string, Object>();
            if (currency != null)
                param.Add("currency", currency);

            return QueryPrivate("getdepositaddress", param);
        }
        public IRestResponse getDepositeHistory(string currency = null)
        {
            var param = new Dictionary<string, Object>();
            if (currency != null)
                param.Add("currency", currency);

            return QueryPrivate("getdeposithistory", param);
        }
        public IRestResponse getWithdrawHistory(string currency = null)
        {
            var param = new Dictionary<string, Object>();
            if (currency != null)
                param.Add("currency", currency);

            return QueryPrivate("getwithdrawalhistory", param);
        }


    }
}
