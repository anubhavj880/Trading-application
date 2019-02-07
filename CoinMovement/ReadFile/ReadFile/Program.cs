using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Collections.ObjectModel;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Exchange;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.Azure.KeyVault.Models;

namespace ReadFile
{
    class Program
    {
        static void Main(string[] args)
        {
            KeyBundle keyBundle = null;
            CreateKV docDb = ServiceProxy.Create<CreateKV>(new Uri(uriString: "fabric:/TradingBackEndApp/Stateful1"), new ServicePartitionKey(0));
            Console.Write("Please  enter Exchange  name:");
            var keyName = Console.ReadLine();
            Console.Write("Please  enter Exchange  ApiKey:");
            var ApiKey = Console.ReadLine();
            Console.Write("Please  enter Exchange  ApiSecret:");


            var appsecret = Console.ReadLine();
            
            try
            {
                var task =Task.Run(async ()=> {
                    var data = await docDb.CreateKey(keyBundle, ApiKey, appsecret, keyName, args);
                   });
              
                task.Wait();
              

           }
            catch (Exception e)
            {
                throw new Exception(e.Message);
            }

        }
      
    }
}
