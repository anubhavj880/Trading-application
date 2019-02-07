using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Bitfinex
{
    class Bitfinex
    {
        private readonly string _url;
        private readonly string _key;
        private readonly string _secret;
        private const string ApiBfxKey = "X-BFX-APIKEY";
        private const string ApiBfxPayload = "X-BFX-PAYLOAD";
        private const string ApiBfxSig = "X-BFX-SIGNATURE";

        /// <summary>
        /// Initializes a new instance of the <see cref="Bitfinex"/> class.
        /// </summary>
        /// <param name="key">The API key.</param>
        /// <param name="secret">The API secret.</param>

        public Bitfinex(string key, string secret)
        {
            _url = "https://api.bitfinex.com";
            _key = key;
            _secret = secret;

        }

        private RestClient GetRestClient(string requestUrl)
        {
            var client = new RestClient();
            var url = _url + requestUrl;
            client.BaseUrl = new System.Uri(url);
            return client;
        }
        private RestRequest GetRestRequest(object obj)
        {
            var jsonObj = JsonConvert.SerializeObject(obj);
            var payload = Convert.ToBase64String(Encoding.UTF8.GetBytes(jsonObj));
            var request = new RestRequest();
            request.Method = Method.POST;
            request.AddHeader(ApiBfxKey, _key);
            request.AddHeader(ApiBfxPayload, payload);
            request.AddHeader(ApiBfxSig, GetHexHashSignature(payload));

            return request;
        }
        private string GetHexHashSignature(string payload)
        {
            HMACSHA384 hmac = new HMACSHA384(Encoding.UTF8.GetBytes(_secret));
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
        private IRestResponse QueryPrivate(string method, Dictionary<string, Object> param = null)
        {
            string path = string.Format(CultureInfo.InvariantCulture, "/v1/0}", method);
            var balancePost = new BitfinexPostBase();
            balancePost.Request = path;
            balancePost.Nonce = GetNonce().ToString();

            var client = GetRestClient(path);
            var restRequest = GetRestRequest(balancePost);
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
        public IRestResponse GetAccountBalance()
        {
            return QueryPrivate("balances");

        }
        public IRestResponse getAccountInfo()
        {
            return QueryPrivate("account_infos");
        }
        public IRestResponse getAccountFee()
        {
            return QueryPrivate("account_fees");
        }
        public IRestResponse getAccountSummary()
        {
            return QueryPrivate("summary");
        }
        public IRestResponse withdraw(string withdraw_type = null, string walletselected = null, string amount = null, string address = null)
        {
            var param = new Dictionary<string, Object>();
            if (withdraw_type != null)
                param.Add("withdraw_type", withdraw_type);
            if (walletselected != null)
                param.Add("walletselected", walletselected);
            if (amount != null)
                param.Add("amount", amount);
            if (address != null)
                param.Add("address", address);
            /*The wallet to withdraw from, can be “trading”, “exchange”, or “deposit”.*/
            /* withdraw_type can be one of the following['bitcoin', 'litecoin', 'ethereum', 'ethereumc', 'mastercoin', 'zcash', 'monero', 'wire', 'dash', 'ripple', 'eos']*/
            return QueryPrivate("withdraw", param);
        }
        public IRestResponse getDepositeAddress(string method = null, string wallet_name = null, Object renew = null)
        {
            var param = new Dictionary<string, Object>();
            if (method != null)
                param.Add("method", method);
            if (wallet_name != null)
                param.Add("wallet_name", wallet_name);
            if (renew != null)
                param.Add("renew", Convert.ToInt32(renew));

           /*method=Method of deposit (methods accepted: “bitcoin”, “litecoin”, “ethereum”, “mastercoin” (tethers), "ethereumc", "zcash", "monero", "iota").
            wallet_name=Wallet to deposit in (accepted: “trading”, “exchange”, “deposit”). Your wallet needs to already exist
            renew=int32 Default is 0. If set to 1, will return a new unused deposit address*/
            return QueryPrivate("deposit/new", param);
        }
        public IRestResponse transferToWallet(Object amount = null, string currency = null, string walletfrom = null, string walletto = null)
        {
            var param = new Dictionary<string, Object>();
            if (amount != null)
                param.Add("amount", Convert.ToInt32(amount));
            if (currency != null)
                param.Add("wallet_name", currency);
            if (walletfrom != null)
                param.Add("walletfrom", walletfrom);
            if (walletto != null)
                param.Add("walletto", walletto);

           
            return QueryPrivate("transfer", param);
        }

    }
}
