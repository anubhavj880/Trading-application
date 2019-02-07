using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Yobit_Coinmovement_Application
{
    class Program
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static void Main(string[] args)
        {
            Console.WriteLine("Please choose the number from below to perform corresponding options: ");
            Console.WriteLine("1. AllBalances ");
            Console.WriteLine("2. GetDepositAddress ");
            Console.WriteLine("3. withdraw ");
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
                Console.WriteLine("2. GetDepositAddress ");
                Console.WriteLine("3. withdraw ");
               
                enteredData = Console.ReadLine();
            }
            int enteredNumber = Int32.Parse(enteredData);
            Yobit yobitObj = new Yobit();

            switch (enteredNumber)
            {
                case 1:
                    /*AllBalance check*/
                    var balances = yobitObj.GetAccountBalances();
                    log.Info("The  Balance: " + balances.Content);
                    Console.WriteLine("The Trading Balance StatusCode: " + balances.StatusCode);
                    Console.WriteLine("The Trading Balance StatusDescription: " + balances.StatusDescription);
                    break;
               
                case 2:
                    /*getDepositeAddress*/
                    Console.WriteLine("Please Enter the valid currency to get the deposit address:  ");
                    String currencyEntered = Console.ReadLine();
                    Console.WriteLine("Please Enter 0 for existing or 1 for new:  ");
                    String numberEntered = Console.ReadLine();
                    var depositeAddress = yobitObj.getDepositeAddress(currencyEntered, numberEntered);
                    log.Info("The DepositeAddress Response : " + depositeAddress.Content);
                    Console.WriteLine("The DepositeAddress StatusCode: " + depositeAddress.StatusCode);
                    Console.WriteLine("The DepositeAddress StatusDescription: " + depositeAddress.StatusDescription);
                    break;
                
                case 3:
                    /*withdraw*/
                    Console.WriteLine("Please Enter the Currency to withdraw:  ");
                    String withdrawCurrency = Console.ReadLine();
                    Console.WriteLine("Please Enter the Quantity:  ");
                    String quantity = Console.ReadLine();
                    Console.WriteLine("Please Enter the address:  ");
                    String address = Console.ReadLine();
                    var withdrawResponse = yobitObj.withdraw(withdrawCurrency, quantity, address);
                    log.Info("The withdraw Response : " + withdrawResponse.Content);
                    Console.WriteLine("The withdraw StatusCode: " + withdrawResponse.StatusCode);
                    Console.WriteLine("The withdraw StatusDescription: " + withdrawResponse.StatusDescription);
                    break;
                
                default:
                    Console.WriteLine("Invalid grade");
                    break;
            }
            Console.ReadLine();

        }
    }
}