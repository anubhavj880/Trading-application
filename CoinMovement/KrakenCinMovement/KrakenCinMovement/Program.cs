using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace KrakenCoinMovement
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static string apiKey = "dDc2G+rS5HMF7CJ9GQmcdkdXUcejpYLBM03tt5pqP9i61B1uM/4zTwfb";
        private static string secretKey = "zdCaXx2ziwh0R8oTOtgW2RnlidLMQG/aXJwXx8wcEcuuI8zo7HDpNuLF5WHaSafy96FzQWO+UdpfqfLra2frPA==";
    
        public static void Main(string[] args)
        {
            Kraken kraken = new Kraken(apiKey, secretKey, 2500);
            var accountBalance = kraken.GetAccountBalance();
            log.Info(accountBalance);
            var tradeBalance = kraken.GetTradeBalance(aclass:"",asset:"");
            /*var depositMethods = kraken.GetDepositMethods( aclass:"",asset: "");
            var depositAddresses = kraken.GetDepositAddresses(asset: "",method: "");
            var depositStatus = kraken.GetDepositStatus(asset: "", method: "");
            var withdrawInfo = kraken.GetWithdrawInfo(asset: "", key: "",amount: 0);
            var withdraw = kraken.Withdraw(asset: "", key: "", amount: 0);
            var withdrawStatus = kraken.GetWithdrawStatus(asset: "", method:"");
            var withdrawCancel = kraken.WithdrawCancel(asset: "", refid: "");*/
            Console.ReadLine();

        }
    }
}
