using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Livecoin_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Livecoin livecoinObj = new Livecoin();
        private long tradetime = 0;
        private String uri = "";
        private String topicName = "";
        /* public async Task SendHttpRequest(String uri, String topicName)
         {
             //log.Info("Entering to SendHttpRequest method:   " + Thread.CurrentThread.ManagedThreadId + " - " + topicName);
             this.uri = uri;
             this.topicName = topicName;
             await Task.Run(() =>
             {
                 System.Timers.Timer aTimer = new System.Timers.Timer();
                 aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                 aTimer.Interval = 15000;
                 aTimer.Enabled = true;
             });

         }

         private async void OnTimedEvent(object sender, ElapsedEventArgs ex)
         {

             using (var client = new HttpClient())
             {
                 ServicePointManager.Expect100Continue = true;
                 ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
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

                     if (topicName.Contains("Trade") && (JsonConvert.DeserializeObject<JArray>(exchangeData).Count > 0))
                     {
                         var jsonArray = JsonConvert.DeserializeObject<JArray>(exchangeData);
                         log.Info("Liqui Trade : " + topicName + " : " + jsonArray.ToString(Formatting.None));
                         String[] topicSplit = topicName.Split('-');

                         foreach (var tradeJson in jsonArray)
                         {

                             if (tradeJson["time"].ToObject<long>() > tradetime)
                             {

                                 await SendToKafka(tradeJson.ToString(), topicName);
                             }
                         }
                         tradetime = jsonArray[0]["time"].ToObject<long>();
                         jsonArray = null;

                     }
                     else if(topicName.Contains("Ticker") || topicName.Contains("Order"))
                     {
                         await SendToKafka(exchangeData, topicName);
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

        public async Task getOrderOrTicker(String uri, String topicName)
        {
            this.uri = uri;
            this.topicName = topicName;
            await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 3000;
                aTimer.Enabled = true;
            });
      
        }
        public async Task getTrade(String uri, String topicName)
        {
            this.uri = uri;
            this.topicName = topicName;
            await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 60000;
                aTimer.Enabled = true;
            });

        }
        private async void OnTimedEvent(object sender, ElapsedEventArgs e)
        {
            using (var client = new HttpClient())
            {
                try
                {
                    ServicePointManager.Expect100Continue = true;
                    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17");
                    HttpResponseMessage response = null;
                    try
                    {
                        response = await client.GetAsync(uri);
                    }
                    catch (HttpRequestException ex)
                    {
                        log.Info("Exception occured while: " + topicName);
                        log.Error("The Source of the exception: " + ex.Source);
                        log.Error("The Message of the exception: " + ex.Message);
                    }
                    catch (Exception ex)
                    {
                        log.Info("Exception occured while: " + topicName);
                        log.Error("The Source of the exception: " + ex.Source);
                        log.Error("The Message of the exception: " + ex.Message);
                    }
                    if ((response != null) && response.IsSuccessStatusCode)
                    {
                        String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());
                        if (topicName.Contains("Trade") && (JsonConvert.DeserializeObject<JArray>(exchangeData).Count > 0))
                        {
                            var jsonArray = JsonConvert.DeserializeObject<JArray>(exchangeData);
                            String[] topicSplit = topicName.Split('-');
                            foreach (var tradeJson in jsonArray)
                            {
                                log.Info(tradeJson.ToString());
                                if (tradeJson["time"].ToObject<long>() > tradetime)
                                {

                                    await SendToKafka(tradeJson.ToString(), topicName);
                                }
                            }
                            tradetime = jsonArray[0]["time"].ToObject<long>();
                            jsonArray = null;
                        }
                        else if (topicName.Contains("Ticker") || topicName.Contains("Order"))
                        {
                            await SendToKafka(exchangeData, topicName);
                        }
                    }
                    else
                    {
                        log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                    }
                }
                catch (Exception ex)
                {
                    log.Info("Exception occured while making request to : " + topicName);
                    log.Info("The Message of the exception: " + ex.Message);
                }
            }


        }

        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');
            if (topicSplit.Last().ToLower().Equals("order"))
            {
                await livecoinObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit.Last().ToLower().Equals("trade"))
            {
                await livecoinObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit.Last().ToLower().Equals("ticker"))
            {
                await livecoinObj.createTickerJson(stringData, topic);
            }
        }
    }
}
