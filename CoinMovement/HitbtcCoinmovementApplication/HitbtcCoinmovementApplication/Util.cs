using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace HitbtcCoinmovementApplication
{
    class Util
    {
        private static string apiKey = "598d331eccdba69d2424c0ac5ec0b8c5";
        private static string secretKey = "442cf9f5a2934a303d34fb3dad331042";
        public static RestClient GetRestClient(string requestUrl)
        {
            var client = new RestClient(requestUrl);

            return client;
        }
        public static RestRequest GetRestRequest(RestClient client, String requesturl)
        {
            var request = new RestRequest(requesturl);
            request.AddParameter("nonce", GetNonce());
            request.AddParameter("apikey", apiKey);

            string sign = CalculateSignature(client.BuildUri(request).PathAndQuery, secretKey);
            request.AddHeader("X-Signature", sign);

            return request;
        }

        public static long GetNonce()
        {
            return DateTime.Now.Ticks * 10 / TimeSpan.TicksPerMillisecond; // use millisecond timestamp or whatever you want
        }
        public static IRestResponse GetRestResponse(RestClient client, RestRequest restRequest)
        {
            var response = client.Execute(restRequest);

            return response;
        }
        public static string CalculateSignature(string text, string secretKey)
        {
            using (var hmacsha512 = new HMACSHA512(Encoding.UTF8.GetBytes(secretKey)))
            {
                hmacsha512.ComputeHash(Encoding.UTF8.GetBytes(text));
                return string.Concat(hmacsha512.Hash.Select(b => b.ToString("x2")).ToArray()); // minimalistic hex-encoding and lower case
            }
        }
    }
}
