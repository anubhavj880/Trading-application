﻿using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Microsoft.Azure.KeyVault.Models;
using Exchange;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;

namespace Stateless2
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class Stateless2 : StatelessService
    {
        public Stateless2(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }
        //protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        //{

        //    return new[] { new ServiceInstanceListener(context => this.CreateServiceRemotingListener(context)) };
        //}
        public Task<string> CreateKey(KeyBundle keyBundle, string keyName)
        {
           

            // Store the created key for the next operation if we have a sequence of operations
            return null;
        }

    }
}
