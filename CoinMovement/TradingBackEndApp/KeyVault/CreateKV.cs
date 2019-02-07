using Microsoft.Azure.KeyVault.Models;
using Microsoft.ServiceFabric.Services.Remoting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exchange
{
   public interface CreateKV : IService
    {
        Task<string>  CreateKey(KeyBundle keyBundle, string appkey, string appsecret,string keyName,string[] args);
        Task<string> Encrypt_ApiKey(KeyBundle key, string appkey, string keyname, string[] args);


    }
}
