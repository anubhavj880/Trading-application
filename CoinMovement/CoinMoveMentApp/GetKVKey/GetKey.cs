using Microsoft.Azure.KeyVault.Models;
using Microsoft.ServiceFabric.Services.Remoting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetKVKey
{
    public interface GetKeyName : IService
    {
        Task<string>  GetKey(string appkey,string appsecret);

    }
}
