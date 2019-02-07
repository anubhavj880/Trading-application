using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace KrakenCoinMovement
{

    public class Kraken
    {
        private readonly string _url;
        private readonly int _version;
        private readonly string _key;
        private readonly string _secret;
        private readonly int _rateLimitMilliseconds = 5000;

        /// <summary>
        /// Initializes a new instance of the <see cref="Kraken"/> class.
        /// </summary>
        /// <param name="key">The API key.</param>
        /// <param name="secret">The API secret.</param>
        /// <param name="rateLimitMilliseconds">The rate limit in milliseconds.</param>
        public Kraken(string key, string secret, int rateLimitMilliseconds = 5000)
        {
            _url = "https://api.kraken.com";
            _version = 0;
            _key = key;
            _secret = secret;
            _rateLimitMilliseconds = rateLimitMilliseconds;
        }

        private string BuildPostData(Dictionary<string, string> param)
        {
            if (param == null)
                return "";

            StringBuilder b = new StringBuilder();
            foreach (var item in param)
                b.Append(string.Format("&{0}={1}", item.Key, item.Value));

            try { return b.ToString().Substring(1); }
            catch (Exception) { return ""; }
        }

        private string QueryPrivate(string method, Dictionary<string, string> param = null)
        {
            RateLimit();

            // generate a 64 bit nonce using a timestamp at tick resolution
            Int64 nonce = DateTime.Now.Ticks * 10 / TimeSpan.TicksPerMillisecond;

            string postData = BuildPostData(param);
            if (!String.IsNullOrEmpty(postData))
                postData = "&" + postData;
            postData = "nonce=" + nonce + postData;

            string path = string.Format(CultureInfo.InvariantCulture, "/{0}/private/{1}", _version, method);
            string address = _url + path;
            HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(address);
            webRequest.ContentType = "application/x-www-form-urlencoded";
            webRequest.Method = "POST";

            AddHeaders(webRequest, nonce, postData, path);

            if (postData != null)
            {
                using (var writer = new StreamWriter(webRequest.GetRequestStream()))
                    writer.Write(postData);
            }

            //Make the request
            try
            {
                using (WebResponse webResponse = webRequest.GetResponse())
                {
                    Stream str = webResponse.GetResponseStream();
                    using (StreamReader sr = new StreamReader(str))
                        return sr.ReadToEnd();
                }
            }
            catch (WebException wex)
            {
                using (HttpWebResponse response = (HttpWebResponse)wex.Response)
                {
                    Stream str = response.GetResponseStream();
                    if (str == null)
                        throw;

                    using (StreamReader sr = new StreamReader(str))
                    {
                        if (response.StatusCode != HttpStatusCode.InternalServerError)
                            throw;
                        return sr.ReadToEnd();
                    }
                }
            }
        }

        private void AddHeaders(HttpWebRequest webRequest, Int64 nonce, string postData, string path)
        {
            webRequest.Headers.Add("API-Key", _key);

            byte[] base64DecodedSecred = Convert.FromBase64String(_secret);

            var np = nonce + Convert.ToChar(0) + postData;

            var pathBytes = Encoding.UTF8.GetBytes(path);
            var hash256Bytes = sha256_hash(np);
            var z = new byte[pathBytes.Count() + hash256Bytes.Count()];
            pathBytes.CopyTo(z, 0);
            hash256Bytes.CopyTo(z, pathBytes.Count());

            var signature = getHash(base64DecodedSecred, z);

            webRequest.Headers.Add("API-Sign", Convert.ToBase64String(signature));
        }


        /// <summary>
        /// Gets the account balance.
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, decimal> GetAccountBalance()
        {
            var res = QueryPrivate("Balance");
            var ret = JsonConvert.DeserializeObject<GetBalanceResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

        /// <summary>
        /// Gets the trade balance.
        /// </summary>
        /// <param name="aclass">The asset class (optional) currency (default)</param>
        /// <param name="asset">Base asset used to determine balance (default = ZUSD).</param>
        /// <returns></returns>
        /// <exception cref="KrakenException"></exception>
        public TradeBalanceInfo GetTradeBalance(string aclass = null, string asset = null)
        {
            var param = new Dictionary<string, string>();
            if (aclass != null)
                param.Add("aclass", aclass);
            if (asset != null)
                param.Add("asset", asset);

            var res = QueryPrivate("TradeBalance");
            var ret = JsonConvert.DeserializeObject<GetTradeBalanceResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }


        /// <summary>
        /// Gets the deposit methods.
        /// </summary>
        /// <param name="aclass">
        /// Asset class (optional):
        /// currency(default).</param>
        /// <param name="asset">Asset being deposited.</param>
        public GetDepositMethodsResult[] GetDepositMethods(string aclass = null, string asset = null)
        {
            var param = new Dictionary<string, string>();
            if (aclass != null)
                param.Add("aclass", aclass);
            if (asset != null)
                param.Add("asset", asset);

            var res = QueryPrivate("DepositMethods", param);
            var ret = JsonConvert.DeserializeObject<GetDepositMethodsResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

        /// <summary>
        /// Gets the deposit addresses.
        /// </summary>
        /// <param name="asset">Asset being deposited.</param>
        /// <param name="method">Name of the deposit method.</param>
        /// <param name="aclass">
        /// Asset class (optional):
        /// currency(default).</param>
        /// <param name="new">Whether or not to generate a new address (optional.  default = false).</param>
        public GetDepositAddressesResult GetDepositAddresses(string asset, string method, string aclass = null, bool? @new = null)
        {
            var param = new Dictionary<string, string>();
            param.Add("asset", asset);
            param.Add("method", method);
            if (aclass != null)
                param.Add("aclass", aclass);
            if (@new != null)
                param.Add("new", @new.ToString().ToLower());

            var res = QueryPrivate("DepositAddresses", param);
            var ret = JsonConvert.DeserializeObject<GetDepositAddressesResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

        /// <summary>
        /// Gets the deposit status.
        /// </summary>
        /// <param name="asset">Asset being deposited.</param>
        /// <param name="method">Name of the deposit method.</param>
        /// <param name="aclass">Asset class (optional):
        /// currency(default).</param>
        /// <returns></returns>
        public GetDepositStatusResult[] GetDepositStatus(string asset, string method, string aclass = null)
        {
            var param = new Dictionary<string, string>();
            param.Add("asset", asset);
            param.Add("method", method);
            if (aclass != null)
                param.Add("aclass", aclass);

            var res = QueryPrivate("DepositStatus", param);
            var ret = JsonConvert.DeserializeObject<GetDepositStatusResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

        /// <summary>
        /// Gets the withdraw information.
        /// </summary>
        /// <param name="asset">Asset being withdrawn.</param>
        /// <param name="key">Withdrawal key name, as set up on your account.</param>
        /// <param name="amount">Amount to withdraw.</param>
        /// <param name="aclass">Asset class (optional):
        /// currency(default).</param>
        /// <returns></returns>
        public GetWithdrawInfoResult GetWithdrawInfo(string asset, string key, decimal amount, string aclass = null)
        {
            var param = new Dictionary<string, string>();
            param.Add("asset", asset);
            param.Add("key", key);
            param.Add("amount", amount.ToString(CultureInfo.InvariantCulture));
            if (aclass != null)
                param.Add("aclass", aclass);

            var res = QueryPrivate("WithdrawInfo", param);
            var ret = JsonConvert.DeserializeObject<GetWithdrawInfoResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

        /// <summary>
        /// Withdraws the specified asset.
        /// </summary>
        /// <param name="asset">Asset being withdrawn.</param>
        /// <param name="key">Withdrawal key name, as set up on your account.</param>
        /// <param name="amount">Amount to withdraw.</param>
        /// <param name="aclass">Asset class (optional):
        /// currency(default).</param>
        /// <returns>The reference id.</returns>
        public string Withdraw(string asset, string key, decimal amount, string aclass = null)
        {
            var param = new Dictionary<string, string>();
            param.Add("asset", asset);
            param.Add("key", key);
            param.Add("amount", amount.ToString(CultureInfo.InvariantCulture));
            if (aclass != null)
                param.Add("aclass", aclass);

            var res = QueryPrivate("Withdraw", param);
            var ret = JsonConvert.DeserializeObject<WithdrawResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result.RefId;
        }

        /// <summary>
        /// Gets the withdraw status.
        /// </summary>
        /// <param name="asset">Asset being withdrawn.</param>
        /// <param name="method">Withdrawal method name (optional).</param>
        /// <param name="aclass">Asset class (optional):
        /// currency(default).</param>
        /// <returns></returns>
        public GetWithdrawStatusResult GetWithdrawStatus(string asset, string method, string aclass = null)
        {
            var param = new Dictionary<string, string>();
            param.Add("asset", asset);
            param.Add("method", method);
            if (aclass != null)
                param.Add("aclass", aclass);

            var res = QueryPrivate("WithdrawStatus", param);
            var ret = JsonConvert.DeserializeObject<GetWithdrawStatusResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

        /// <summary>
        /// Cancel the withdrawal.
        ///
        /// Note: Cancelation cannot be guaranteed. This will put in a cancelation request.
        /// Depending upon how far along the withdrawal process is, it may not be possible to cancel the withdrawal.
        /// </summary>
        /// <param name="asset">Asset being withdrawn.</param>
        /// <param name="refid">Withdrawal reference id.</param>
        /// <param name="aclass">Asset class (optional):
        /// currency(default).</param>
        public bool WithdrawCancel(string asset, string refid, string aclass = null)
        {
            var param = new Dictionary<string, string>();
            param.Add("asset", asset);
            param.Add("refid", refid);
            if (aclass != null)
                param.Add("aclass", aclass);

            var res = QueryPrivate("WithdrawCancel", param);
            var ret = JsonConvert.DeserializeObject<WithdrawCancelResponse>(res);
            if (ret.Error.Count != 0)
                throw new KrakenException(ret.Error[0], ret);
            return ret.Result;
        }

       

        #region Helper methods

        private byte[] sha256_hash(String value)
        {
            using (SHA256 hash = SHA256Managed.Create())
                return hash.ComputeHash(Encoding.UTF8.GetBytes(value));
        }

        private byte[] getHash(byte[] keyByte, byte[] messageBytes)
        {
            using (var hmacsha512 = new HMACSHA512(keyByte))
                return hmacsha512.ComputeHash(messageBytes);
        }

        #endregion Helper methods

        #region Rate limiter

        private long lastTicks = 0;
        private object thisLock = new object();

        private void RateLimit()
        {
            lock (thisLock)
            {
                long elapsedTicks = DateTime.Now.Ticks - lastTicks;
                TimeSpan elapsedSpan = new TimeSpan(elapsedTicks);
                if (elapsedSpan.TotalMilliseconds < _rateLimitMilliseconds)
                    Thread.Sleep(_rateLimitMilliseconds - (int)elapsedSpan.TotalMilliseconds);
                lastTicks = DateTime.Now.Ticks;
            }
        }

        #endregion Rate limiter
    }
}

