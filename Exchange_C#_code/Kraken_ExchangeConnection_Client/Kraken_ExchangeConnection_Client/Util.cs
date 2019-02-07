using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Kraken_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Kraken krakenObj = new Kraken();
        private String krakenLastId = null;
        private String uri = "";
        private String topicName = "";
        public async Task SendHttpRequest(String uri, String topicName)
        {
            //log.Info("Entering to SendHttpRequest method:   " + Thread.CurrentThread.ManagedThreadId + " - " + topicName);
            this.uri = uri;
            this.topicName = topicName;
            await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 1000;
                aTimer.Enabled = true;
            });

        }

        private async void OnTimedEvent(object sender, ElapsedEventArgs ex)
        {

            using (var client = new HttpClient())
            {
                try
                {
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17");
                    HttpResponseMessage response = null;
                    try
                    {
                        if (topicName.Contains("Trade"))
                        {

                            response = await client.GetAsync(uri + "&since=" + krakenLastId);
                        }
                        else
                        {
                            response = await client.GetAsync(uri);
                        }
                    }
                    catch (HttpRequestException e)
                    {
                        log.Info("Exception occured while HttpRequest: " + topicName);
                        log.Error("The Source of the exception: " + e.Source);
                        log.Error("The StackTrace of the exception: " + e.StackTrace);
                    }


                    if ((response != null) && response.IsSuccessStatusCode)
                    {
                        String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());
                        IDictionary<string, JToken> jsondata = JsonConvert.DeserializeObject<JObject>(exchangeData);
                        if (topicName.Contains("Trade") && (jsondata.ContainsKey("result")))
                        {
                            
                           
                            krakenLastId = jsondata["result"]["last"].ToObject<String>();
                            await SendToKafka(exchangeData, topicName);
                        }
                        else if((topicName.Contains("Order") || topicName.Contains("Ticker"))&&(jsondata.ContainsKey("result")))
                        {
                            await SendToKafka(exchangeData, topicName);
                        }
                        else
                        {
                            log.Error("The error received from exchange : " + exchangeData);
                        }
                    }
                    else
                    {
                        log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                    }
                }
                catch (Exception e)
                {
                    log.Info("Exception occured while processing the request to : " + topicName);
                    log.Info("The Source of the exception: " + e.Source);
                    log.Info("The Message of the exception: " + e.Message);
                }
            }
        }



        /* public async Task SendHttpRequest(String uri, String topicName)
         {
             this.uri = uri;
             this.topicName = topicName;
             while (true)
             {
                 using (var client = new HttpClient())
                 {
                     try
                     {
                         client.DefaultRequestHeaders.Accept.Clear();
                         client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                         client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17");
                         HttpResponseMessage response = null;
                         try
                         {
                             if (topicName.Contains("Trade"))
                             {

                                 response = await client.GetAsync(uri + "&since=" + krakenLastId);
                             }
                             else
                             {
                                 response = await client.GetAsync(uri);
                             }
                         }
                         catch (HttpRequestException e)
                         {
                             log.Info("Exception occured while: " + topicName);
                             log.Info("The Source of the exception: " + e.Source);
                             log.Info("The Message of the exception: " + e.Message);
                         }


                         if ((response != null) && response.IsSuccessStatusCode)
                         {
                             String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());

                             if (topicName.Contains("Trade"))
                             {
                                 var jsondata = JsonConvert.DeserializeObject<JObject>(exchangeData);

                                 krakenLastId = jsondata["result"]["last"].ToObject<String>();
                                 await SendToKafka(exchangeData, topicName);
                             }
                             else
                             {
                                 await SendToKafka(exchangeData, topicName);
                             }
                         }
                         else
                         {
                             log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                         }
                     }
                     catch (Exception e)
                     {
                         log.Info("Exception occured while processing the request to : " + topicName);
                         log.Info("The Source of the exception: " + e.Source);
                         log.Info("The Message of the exception: " + e.Message);
                     }
                 }
             }
         }*/
        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');

            if (topicSplit[2].ToLower().Equals("order"))
            {
                await krakenObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await krakenObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await krakenObj.createTickerJson(stringData, topic);
            }
        }
    }
}
