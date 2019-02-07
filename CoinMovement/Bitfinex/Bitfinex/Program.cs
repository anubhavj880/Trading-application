using log4net;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;

namespace Bitfinex
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
 
        private static string apiKey = "NqRSVxPK0Bc1OfWRv9m66KsAaPnabyCiS8smOBPBhtj";
        private static string secretKey = "sjgzotlsKxjuZww5Vc2nAWtJTeiGRbQOCaSEH9qO3xS";
           
        static void Main(string[] args)
        {
            Bitfinex bitfinexObj = new Bitfinex(apiKey,secretKey);
            Console.WriteLine("Please choose the number from below to perform corresponding options: ");
            Console.WriteLine("1. Balance ");
            Console.WriteLine("2. accountInfo ");
            Console.WriteLine("3. accountFee ");
            Console.WriteLine("4. accountSummary ");
            Console.WriteLine("5. withdraw ");
            Console.WriteLine("6. getDepositeAddress ");
            Console.WriteLine("7. TransferToWallet ");
            int requirednum;
            String enteredData = Console.ReadLine();
            Boolean flag = true;
            while (flag)
            {
                if (int.TryParse(enteredData, out requirednum) && Int32.Parse(enteredData) >= 1 && Int32.Parse(enteredData) <= 8)
                {
                    break;
                }
                Console.WriteLine("Try again: Entered value is incorrect");
                Console.WriteLine("Please choose valid number from below to perform corresponding options: ");
                Console.WriteLine("1. Balance ");
                Console.WriteLine("2. accountInfo ");
                Console.WriteLine("3. accountFee ");
                Console.WriteLine("4. accountSummary ");
                Console.WriteLine("5. withdraw ");
                Console.WriteLine("6. getDepositeAddress ");
                Console.WriteLine("7. TransferToWallet ");
                enteredData = Console.ReadLine();
            }
            int enteredNumber = Int32.Parse(enteredData);


            switch (enteredNumber)
            {
                case 1:
                    /*Balance check*/
                    var balance = bitfinexObj.GetAccountBalance();
                    log.Info("The  Balance: " + balance.Content);
                    Console.WriteLine("The Trading Balance StatusCode: " + balance.StatusCode);
                    Console.WriteLine("The Trading Balance StatusDescription: " + balance.StatusDescription);
                    break;
                case 2:
                    /*AccountInfo check*/
                    var accountInfoResponse = bitfinexObj.getAccountInfo();
                    log.Info("The accountInfoResponce: " + accountInfoResponse.Content);
                    Console.WriteLine("The accountInfoResponce StatusCode: " + accountInfoResponse.StatusCode);
                    Console.WriteLine("The accountInfoResponce StatusDescription: " + accountInfoResponse.StatusDescription);
                    break;
                case 3:
                    /*AccountFee check*/
                    var accountFeeResponse = bitfinexObj.getAccountFee();
                    log.Info("The accountFeeResponse: " + accountFeeResponse.Content);
                    Console.WriteLine("The accountFeeResponse StatusCode: " + accountFeeResponse.StatusCode);
                    Console.WriteLine("The accountFeeResponse StatusDescription: " + accountFeeResponse.StatusDescription);
                    break;
                case 4:
                    /*Account Summary*/
                    var accountSummary = bitfinexObj.getAccountSummary();
                    log.Info("The accountSummary Response : " + accountSummary.Content);
                    Console.WriteLine("The accountSummary StatusCode: " + accountSummary.StatusCode);
                    Console.WriteLine("The accountSummary StatusDescription: " + accountSummary.StatusDescription);
                    break;
                case 5:
                    /*withdraw*/
                    Console.WriteLine("Please Enter the withdraw_type:  ");
                    String withdraw_type = Console.ReadLine();
                    Console.WriteLine("Please Enter the walletselected:  ");
                    String walletselected = Console.ReadLine();
                    Console.WriteLine("Please Enter the amount:  ");
                    String amount = Console.ReadLine();
                    Console.WriteLine("Please Enter the address:  ");
                    String address = Console.ReadLine();
                    var withdrawResponse = bitfinexObj.withdraw(withdraw_type,walletselected,amount,address);
                    log.Info("The withdraw Response : " + withdrawResponse.Content);
                    Console.WriteLine("The withdraw StatusCode: " + withdrawResponse.StatusCode);
                    Console.WriteLine("The withdraw StatusDescription: " + withdrawResponse.StatusDescription);
                    break;
                case 6:
                    /*getDepositeAddress*/
                    Console.WriteLine("Please Enter the method:  ");
                    String method = Console.ReadLine();
                    Console.WriteLine("Please Enter the wallet_name:  ");
                    String wallet_name = Console.ReadLine();
                    Console.WriteLine("Please Enter the 0 to get existing address or 1 to renew :  ");
                    String renew = Console.ReadLine();

                    var depositeAddress = bitfinexObj.getDepositeAddress(method, wallet_name, renew);
                    log.Info("The withdraw Response : " + depositeAddress.Content);
                    Console.WriteLine("The withdraw StatusCode: " + depositeAddress.StatusCode);
                    Console.WriteLine("The withdraw StatusDescription: " + depositeAddress.StatusDescription);
                    break;
                case 7:
                    /*TransferToWallet*/
                    Console.WriteLine("Please Enter the amount:  ");
                    String amountToTransfer = Console.ReadLine();
                    Console.WriteLine("Please Enter the currency:  ");
                    String currency = Console.ReadLine();
                    Console.WriteLine("Please Enter the walletfrom :  ");
                    String walletfrom = Console.ReadLine();
                    Console.WriteLine("Please Enter the walletto :  ");
                    String walletto = Console.ReadLine();
                    var TransferToWallet = bitfinexObj.transferToWallet(amountToTransfer, currency, walletfrom, walletto);
                    log.Info("The TransferToWallet Response : " + TransferToWallet.Content);
                    Console.WriteLine("The TransferToWallet Response StatusCode: " + TransferToWallet.StatusCode);
                    Console.WriteLine("The TransferToWallet Response StatusDescription: " + TransferToWallet.StatusDescription);
                    break;
                
                default:
                    Console.WriteLine("Invalid grade");
                    break;
            }
            Console.ReadLine();

        }
    }
}