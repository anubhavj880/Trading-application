using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HitbtcCoinmovementApplication
{
    class Hitbtc
    {
        public IRestResponse getBalance(String balance)
        {

            var client = Util.GetRestClient("https://api.hitbtc.com");
            var restRequest = Util.GetRestRequest(client, balance);
            restRequest.Method = Method.GET;
            var response = Util.GetRestResponse(client, restRequest);
            return response;
        }
        public IRestResponse getActiveOrders(String activeOrderUrl)
        {

            var client = Util.GetRestClient("https://api.hitbtc.com");
            var restRequest = Util.GetRestRequest(client, activeOrderUrl);
            restRequest.Method = Method.GET;
            var response = Util.GetRestResponse(client, restRequest);
            return response;
        }
        public IRestResponse transferToMainOrTradingAcc(String transferUrl, Decimal amount, String currencyCode)
        {

            var client = Util.GetRestClient("https://api.hitbtc.com");
            var restRequest = Util.GetRestRequest(client, transferUrl);
            restRequest.Method = Method.POST;
            restRequest.AddParameter("amount", amount);
            restRequest.AddParameter("currency_code", currencyCode);
            var response = Util.GetRestResponse(client, restRequest);
            return response;
        }

        public IRestResponse withdrawFund(String transferUrl, Decimal amount, String currencyCode, String address)
        {

            var client = Util.GetRestClient("https://api.hitbtc.com");
            var restRequest = Util.GetRestRequest(client, transferUrl);
            restRequest.Method = Method.POST;
            restRequest.AddParameter("amount", amount);
            restRequest.AddParameter("currency_code", currencyCode);
            restRequest.AddParameter("address", address);
            var response = Util.GetRestResponse(client, restRequest);
            return response;
        }
        public IRestResponse getWalletAddress(String transferUrl, String currencyCode)
        {

            var client = Util.GetRestClient("https://api.hitbtc.com");
            var restRequest = Util.GetRestRequest(client, transferUrl + "/" + currencyCode);
            restRequest.Method = Method.GET;
            var response = Util.GetRestResponse(client, restRequest);
            return response;
        }
        public IRestResponse createWalletAddress(String transferUrl, String currencyCode)
        {

            var client = Util.GetRestClient("https://api.hitbtc.com");
            var restRequest = Util.GetRestRequest(client, transferUrl + "/" + currencyCode);
            restRequest.Method = Method.POST;
            var response = Util.GetRestResponse(client, restRequest);
            return response;
        }
    }
}
