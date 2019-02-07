using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BittrexCoinmovementApplication
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        
        static void Main(string[] args)
        {
            Console.WriteLine("Please choose the number from below to perform corresponding options: ");
            Console.WriteLine("1. AllBalances ");
            Console.WriteLine("2. SingleCurrencyBalance ");
            Console.WriteLine("3. GetDepositAddress ");
            Console.WriteLine("4. GetDepositHistory ");
            Console.WriteLine("5. withdraw ");
            Console.WriteLine("6. GetWithdrawHistory ");
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
                Console.WriteLine("1. AllBalances ");
                Console.WriteLine("2. SingleCurrencyBalance ");
                Console.WriteLine("3. GetDepositAddress ");
                Console.WriteLine("4. GetDepositHistory ");
                Console.WriteLine("5. withdraw ");
                Console.WriteLine("6. GetWithdrawHistory ");
                enteredData = Console.ReadLine();
            }
            int enteredNumber = Int32.Parse(enteredData);
            Bittrex bittrexObj = new Bittrex();

            switch (enteredNumber)
            {
                case 1:
                    /*AllBalance check*/
                    var balances = bittrexObj.GetAccountBalances();
                    log.Info("The  Balance: " + balances.Content);
                    Console.WriteLine("The Trading Balance StatusCode: " + balances.StatusCode);
                    Console.WriteLine("The Trading Balance StatusDescription: " + balances.StatusDescription);
                    break;
                case 2:
                    /*SingleCurrencyBalance check*/
                    Console.WriteLine("Please Enter the vaild curreny:  ");
                    String currency = Console.ReadLine();
                    var balance = bittrexObj.GetAccountBalance(currency);
                    log.Info("The SingleCurrencyBalance: " + balance.Content);
                    Console.WriteLine("The SingleCurrencyBalance StatusCode: " + balance.StatusCode);
                    Console.WriteLine("The SingleCurrencyBalance StatusDescription: " + balance.StatusDescription);
                    break;
                case 3:
                    /*getDepositeAddress*/
                    Console.WriteLine("Please Enter the valid currency to get the deposit address:  ");
                    String currencyEntered = Console.ReadLine();
                    var depositeAddress = bittrexObj.getDepositeAddress(currencyEntered);
                    log.Info("The DepositeAddress Response : " + depositeAddress.Content);
                    Console.WriteLine("The DepositeAddress StatusCode: " + depositeAddress.StatusCode);
                    Console.WriteLine("The DepositeAddress StatusDescription: " + depositeAddress.StatusDescription);
                    break;
                case 4:
                    /*GetDepositHistory*/
                    Console.WriteLine("Please Enter the valid currency to get the deposit history fro tha currency:  ");
                    String depositeCurrency = Console.ReadLine();
                    var depositeHistory = bittrexObj.getDepositeHistory(depositeCurrency);
                    log.Info("The depositeHistory Response : " + depositeHistory.Content);
                    Console.WriteLine("The depositeHistory StatusCode: " + depositeHistory.StatusCode);
                    Console.WriteLine("The depositeHistory StatusDescription: " + depositeHistory.StatusDescription);
                    break;
                case 5:
                    /*withdraw*/
                    Console.WriteLine("Please Enter the Currency to withdraw:  ");
                    String withdrawCurrency = Console.ReadLine();
                    Console.WriteLine("Please Enter the Quantity:  ");
                    String quantity = Console.ReadLine();
                    Console.WriteLine("Please Enter the address:  ");
                    String address = Console.ReadLine();
                    var withdrawResponse = new Bittrex().withdraw(withdrawCurrency, quantity, address);
                    log.Info("The withdraw Response : " + withdrawResponse.Content);
                    Console.WriteLine("The withdraw StatusCode: " + withdrawResponse.StatusCode);
                    Console.WriteLine("The withdraw StatusDescription: " + withdrawResponse.StatusDescription);
                    break;
                case 6:
                    /*GetWithdrawHistory*/
                    Console.WriteLine("Please Enter the currency to get withdraw history:  ");
                     String currencyWithrawHis = Console.ReadLine();
                     var withdrawHistory = bittrexObj.getWithdrawHistory(currencyWithrawHis);
                     log.Info("The withdrawHistory Response : " + withdrawHistory.Content);
                     Console.WriteLine("The withdrawHistory StatusCode: " + withdrawHistory.StatusCode);
                     Console.WriteLine("The withdrawHistory StatusDescription: " + withdrawHistory.StatusDescription);
                    break;
                default:
                    Console.WriteLine("Invalid grade");
                    break;
            }
            Console.ReadLine();

        }
    }
}