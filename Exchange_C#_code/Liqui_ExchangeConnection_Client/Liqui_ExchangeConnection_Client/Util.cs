﻿using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Liqui_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Liqui liquiObj = new Liqui();
        private long tradeId = 0;
        private String uri = "";
        private String topicName = "";
        /*   public async Task SendHttpRequest(String uri, String topicName)
           {
               //log.Info("Entering to SendHttpRequest method:   " + Thread.CurrentThread.ManagedThreadId + " - " + topicName);
               this.uri = uri;
               this.topicName = topicName;
               await Task.Run(() =>
               {
                   System.Timers.Timer aTimer = new System.Timers.Timer();
                   aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                   aTimer.Interval = 12000;
                   aTimer.Enabled = true;
               });

           }

           private async void OnTimedEvent(object sender, ElapsedEventArgs ex)
           {

               using (var client = new HttpClient())
               {
                   client.DefaultRequestHeaders.Accept.Clear();
                   client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                   client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17");
                   HttpResponseMessage response = null;
                   try
                   {
                       response = await client.GetAsync(uri);
                   }
                   catch (HttpRequestException e)
                   {
                       log.Info("Exception occured while: " + topicName);
                       log.Error("The Source of the exception: " + e.Source);
                       log.Error("The StackTrace of the exception: " + e.StackTrace);
                   }
                   catch (Exception e)
                   {
                       log.Info("Exception occured while: " + topicName);
                       log.Error("The Source of the exception: " + e.Source);
                       log.Error("The StackTrace of the exception: " + e.StackTrace);
                   }

                   if ((response != null) && response.IsSuccessStatusCode)
                   {
                       String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());
                        if (topicName.Contains("Trade"))
                       {
                           var jsonData = JsonConvert.DeserializeObject<JObject>(exchangeData);
                           log.Info("Liqui Trade : " + topicName + " : " + jsonData.ToString(Formatting.None));
                           Dictionary<string, object> values = JsonConvert.DeserializeObject<Dictionary<string, object>>(exchangeData);
                           String[] topicSplit = topicName.Split('-');
                           if (values.Keys.Contains(topicSplit[1].ToLower()))
                           {
                               var jsonArray = jsonData[topicSplit[1].ToLower()].ToObject<JArray>();

                               foreach (var tradeJson in jsonArray)
                               {

                                   if (tradeJson["tid"].ToObject<long>() > tradeId)
                                   {

                                       await SendToKafka(tradeJson.ToString(), topicName);
                                   }
                               }
                               tradeId = jsonArray[0]["tid"].ToObject<long>();
                               jsonData = null;
                               jsonArray = null;
                           }
                       }
                       else
                       {
                           Dictionary<string, object> values = JsonConvert.DeserializeObject<Dictionary<string, object>>(exchangeData);
                           String[] topicSplit = topicName.Split('-');
                           if (values.Keys.Contains(topicSplit[1].ToLower()))
                           {
                               await SendToKafka(exchangeData, topicName);
                           }
                       }
                       exchangeData = null;
                       response = null;
                   }
                   else
                   {
                       log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                   }
               }

           }
           */
        public async Task SendHttpRequest(String uri, String topicName)
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
                            response = await client.GetAsync(uri);
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
                                var jsonData = JsonConvert.DeserializeObject<JObject>(exchangeData);

                                Dictionary<string, object> values = JsonConvert.DeserializeObject<Dictionary<string, object>>(exchangeData);
                                String[] topicSplit = topicName.Split('-');
                                if (values.Keys.Contains(topicSplit[1].ToLower()))
                                {
                                    var jsonArray = jsonData[topicSplit[1].ToLower()].ToObject<JArray>();

                                    foreach (var tradeJson in jsonArray)
                                    {

                                        if (tradeJson["tid"].ToObject<long>() > tradeId)
                                        {

                                            await SendToKafka(tradeJson.ToString(), topicName);
                                        }
                                    }
                                    tradeId = jsonArray[0]["tid"].ToObject<long>();
                                    jsonData = null;
                                    jsonArray = null;
                                }
                            }
                            else
                            {
                                Dictionary<string, object> values = JsonConvert.DeserializeObject<Dictionary<string, object>>(exchangeData);
                                String[] topicSplit = topicName.Split('-');
                                if (values.Keys.Contains(topicSplit[1].ToLower()))
                                {
                                    await SendToKafka(exchangeData, topicName);
                                }
                            }
                        }
                        else
                        {
                            log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Info("Exception occured while processing request to : " + topicName);
                        log.Info("The Source of the exception: " + e.Source);
                        log.Info("The Message of the exception: " + e.Message);
                    }
                }
            }
        }
        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');
            if (topicSplit[2].ToLower().Equals("order"))
            {
                await liquiObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await liquiObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await liquiObj.createTickerJson(stringData, topic);
            }
        }
    }
}
