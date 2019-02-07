using log4net;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace HitbtcCoinmovementApplication
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static string TradingBalanceRequest = @"/api/1/trading/balance";//Request: GET /api/1/trading/balance
        private static string PaymentBalanceRequest = @"/api/1/payment/balance";// methos is GET
        private static string TransferFundToTradeAcc = @"/api/1/payment/transfer_to_trading";// method is POST, Parameter => ?amount=0.0001&currency_code=ETH
        private static string TransferFundToMainAcc = @"/api/1/payment/transfer_to_main";// method is POST, Parameter => ?amount=0.0001&currency_code=ETH
        private static string Withdraw = @"/api/1/payment/payout";//Request: POST /api/1/payment/payout, Example: amount=0.001&currency_code=BTC&address=1LuWvENyuPNHsHWjDgU1QYKWUYN9xxy7n5
        private static string listPaymentTransaction = @"/api/1/payment/transactions";//Request: GET /api/1/payment/transactions, Example: /api/1/payment/transactions?limit=10
        private static string PaymentTransactionById = @"/api/1/payment/transactions/";//GET /api/1/payment/transactions/89229-171-97181
        private static string CreateWalletAddress = @"/api/1/payment/address/";// method is POST, Example: POST /api/1/payment/address/BTC
        private static string GetWalletAddress = @"/api/1/payment/address/"; // methos is GET, Example: GET /api/1/payment/address/BTC

        static void Main(string[] args)
        {
            Console.WriteLine("Please choose the number from below to perform corresponding options: ");
            Console.WriteLine("1. GetTradingBalance ");
            Console.WriteLine("2. GetPaymentBalance ");
            Console.WriteLine("3. GetActiveOrders ");
            Console.WriteLine("4. TransferToTradeAccount ");
            Console.WriteLine("5. TransferToMainAccount ");
            Console.WriteLine("6. GetWalletAddress ");
            Console.WriteLine("7. CreateWalletAddress ");
            Console.WriteLine("8. WithdrawFund ");
            int requirednum;
            String enteredData = Console.ReadLine();
            Boolean flag = true;
            while (flag)
            {
                if(int.TryParse(enteredData, out requirednum) && Int32.Parse(enteredData) >= 1 && Int32.Parse(enteredData) <= 8)
                {
                    break;
                }
                Console.WriteLine("Try again: Entered value is incorrect");
                Console.WriteLine("Please choose valid number from below to perform corresponding options: ");
                Console.WriteLine("1. GetTradingBalance ");
                Console.WriteLine("2. GetPaymentBalance ");
                Console.WriteLine("3. TransferToTradeAccount ");
                Console.WriteLine("4. TransferToMainAccount ");
                Console.WriteLine("5. GetWalletAddress ");
                Console.WriteLine("6. CreateWalletAddress ");
                Console.WriteLine("7. WithdrawFund ");
                enteredData = Console.ReadLine();
            }
            int enteredNumber = Int32.Parse(enteredData);


            switch (enteredNumber)
            {
                case 1:
                    /*TradingBalance check*/
                    var tradingBalanceResponce = new Hitbtc().getBalance(TradingBalanceRequest);
                    log.Info("The Trading Balance: " + tradingBalanceResponce.Content);
                    Console.WriteLine("The Trading Balance StatusCode: " + tradingBalanceResponce.StatusCode);
                    Console.WriteLine("The Trading Balance StatusDescription: " + tradingBalanceResponce.StatusDescription);
                    break;
                case 2:
                    /*PaymentBalance check*/
                    var paymentBalanceResponce = new Hitbtc().getBalance(PaymentBalanceRequest);
                    log.Info("The Payment Balance: " + paymentBalanceResponce.Content);
                    Console.WriteLine("The Payment Balance StatusCode: " + paymentBalanceResponce.StatusCode);
                    Console.WriteLine("The Payment Balance StatusDescription: " + paymentBalanceResponce.StatusDescription);
                    break;
                case 3:
                    
                    /*Transfer To Trading Account*/
                    
                    Console.WriteLine("Please Enter the amount to transfer to Trading account:  ");
                    String amount = Console.ReadLine();
                    Console.WriteLine("Please Enter the currency to transfer to Trading account:  ");
                    String currencyCode = Console.ReadLine();
                    var TradingtransferResponce = new Hitbtc().transferToMainOrTradingAcc(TransferFundToTradeAcc, Convert.ToDecimal(amount), currencyCode);
                    log.Info("The TradingTransfer Response : " + TradingtransferResponce.Content);
                    Console.WriteLine("The TradingTransfer StatusCode: " + TradingtransferResponce.StatusCode);
                    Console.WriteLine("The TradingTransfer StatusDescription: " + TradingtransferResponce.StatusDescription);
                    break;
                case 4:
                    /*Transfer To Main Account*/
                    Console.WriteLine("Please Enter the amount to transfer to Main account:  ");
                    String mainAmount = Console.ReadLine();
                    Console.WriteLine("Please Enter the currency to transfer to Main account:  ");
                    String mainCurrencyCode = Console.ReadLine();
                    var MainTransferResponce = new Hitbtc().transferToMainOrTradingAcc(TransferFundToMainAcc, Convert.ToDecimal(mainAmount), mainCurrencyCode);
                    log.Info("The MainTransfer Response : " + MainTransferResponce.Content);
                    Console.WriteLine("The MainTransfer StatusCode: " + MainTransferResponce.StatusCode);
                    Console.WriteLine("The MainTransfer StatusDescription: " + MainTransferResponce.StatusDescription);
                    break;
                case 5:
                    /*get wallet address*/
                    Console.WriteLine("Please Enter the wallet currency code to get the address:  ");
                    String walletCurrencyCode = Console.ReadLine();
                    var walletAddressResponce = new Hitbtc().getWalletAddress(GetWalletAddress, walletCurrencyCode);
                    log.Info("The WalletAddress Response : " + walletAddressResponce.Content);
                    Console.WriteLine("The WalletAddress Response StatusCode: " + walletAddressResponce.StatusCode);
                    Console.WriteLine("The WalletAddress Response StatusDescription: " + walletAddressResponce.StatusDescription);
                    break;
                case 6:
                    /*Create wallet address*/
                    Console.WriteLine("Please Enter the wallet currency code to create the address:  ");
                    String CurrencyCode = Console.ReadLine();
                    var createWalletAddressResponce = new Hitbtc().createWalletAddress(CreateWalletAddress, CurrencyCode);
                    log.Info("The Created WalletAddress Response : " + createWalletAddressResponce.Content);
                    Console.WriteLine("The Created WalletAddress Response StatusCode: " + createWalletAddressResponce.StatusCode);
                    Console.WriteLine("The Created WalletAddress Response StatusDescription: " + createWalletAddressResponce.StatusDescription);
                    break;
                case 7:
                    /*WithDraw fund*/
                    Console.WriteLine("Please Enter the amount to withdraw:  ");
                    String witdrawAmount = Console.ReadLine();
                    Console.WriteLine("Please Enter the withdraw currency code:  ");
                    String withdrawCurrencyCode = Console.ReadLine();
                    Console.WriteLine("Please Enter address to send:  ");
                    String sendAddress = Console.ReadLine();
                    var withdrawResponce = new Hitbtc().withdrawFund(Withdraw, Convert.ToDecimal(witdrawAmount), withdrawCurrencyCode, sendAddress);
                    log.Info("The withdrawResponce : " + withdrawResponce.Content);
                    Console.WriteLine("The withdrawResponce StatusCode: " + withdrawResponce.StatusCode);
                    Console.WriteLine("The withdrawResponce StatusDescription: " + withdrawResponce.StatusDescription);
                    break;
                default:
                    Console.WriteLine("Invalid grade");
                    break;
            }
             Console.ReadLine();
        }

       


    }
}