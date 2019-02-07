using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using WebSocketSharp;

namespace Coinbase_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Coinbase coinbaseObj = new Coinbase();
        private String uri = "";
        private String topicName = "";
        private static Random randomNumberObj = new Random();
        private long rdmNumber = 0L;
        private WebSocket ws = null;
        public async Task SendHttpRequest(String uri, String topicName)
        {
            //log.Info("Entering to SendHttpRequest method:   " + Thread.CurrentThread.ManagedThreadId + " - " + topicName);
            this.uri = uri;
            this.topicName = topicName;
            /*await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 4000;
                aTimer.Enabled = true;
            });*/
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
                            log.Error("The Source of the exception: " + e.Source);
                            log.Error("The StackTrace of the exception: " + e.StackTrace);
                        }
                        if ((response != null) && response.IsSuccessStatusCode)
                        {
                            String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());

                            await SendToKafka(exchangeData, topicName, rdmNumber);
                            exchangeData = null;
                            response = null;
                        }
                        else
                        {
                            log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Info("Exception occured while: " + topicName);
                        log.Info("The Source of the exception: " + e.Source);
                        log.Info("The Message of the exception: " + e.Message);
                        log.Info("The StackTrace of the exception: " + e.StackTrace);
                    }
                }
            }
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

                    await SendToKafka(exchangeData, topicName, rdmNumber);
                    exchangeData = null;
                    response = null;
                }
                else
                {
                    log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                }
            }

        }

        public async Task webSocketConnect(String subscribeSchema, String uri, String topicName, String restlink)
        {
            await Task.Run(() =>
           {
               ws = new WebSocket(uri);
               /*System.Timers.Timer aTimer = new System.Timers.Timer();
               aTimer.Elapsed += new ElapsedEventHandler(WebsocketOnTimedEvent);
               aTimer.Interval = 900000;
               aTimer.Enabled = true;*/

               ws.OnOpen += async (sender, e) =>
               {
                   rdmNumber = randomNumberObj.Next();
                   log.Info("Websocket Connected for: " + topicName);
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
                               response = await client.GetAsync(restlink);
                           }
                           catch (HttpRequestException ex)
                           {
                               log.Info("Exception occured while: " + topicName);
                               log.Error("The Source of the exception: " + ex.Source);
                               log.Error("The StackTrace of the exception: " + ex.StackTrace);
                           }

                           if ((response != null) && response.IsSuccessStatusCode)
                           {
                               String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());

                               await SendToKafka(exchangeData, "CoinBase-" + topicName + "-Order", rdmNumber);
                               exchangeData = null;
                               response = null;
                           }
                           else
                           {
                               log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                           }
                       }
                       catch (Exception ex)
                       {
                           log.Info("Exception occured while: " + topicName);
                           log.Info("The Source of the exception: " + ex.Source);
                           log.Info("The Message of the exception: " + ex.Message);
                           log.Info("The StackTrace of the exception: " + ex.StackTrace);
                       }
                   }
               };

               ws.EmitOnPing = true;
               ws.OnMessage += async (sender, e) =>
               {
                   var jsonData = JsonConvert.DeserializeObject<JObject>(e.Data);
                   String[] currencyPairSplit = jsonData["product_id"].ToString().Split('-');
                   if (jsonData["type"].ToString().Equals("open") || jsonData["type"].ToString().Equals("done") || jsonData["type"].ToString().Equals("match") || jsonData["type"].ToString().Equals("change"))
                   {

                       await SendToKafka(e.Data, "CoinBase-" + currencyPairSplit[0] + currencyPairSplit[1] + "-Order", rdmNumber);
                   }
                   if (jsonData["type"].ToString().Equals("match"))
                   {

                       await SendToKafka(e.Data, "CoinBase-" + currencyPairSplit[0] + currencyPairSplit[1] + "-Trade", rdmNumber);
                   }
               };
               ws.OnError += (sender, e) =>
               {
                   log.Error("Error occured: " + topicName + " with the message: " + e.Message);
                  

               };
               ws.OnClose += (sender, e) =>
              {
                  log.Info("Websocket getting closed for the following topic : " + topicName + "\n");
                  try
                  {
                      while (!(ws.ReadyState.ToString().Equals("Open")))
                      {
                          log.Info("The State before reconnect :   " + ws.ReadyState);
                          ws.Connect();
                          ws.Send(subscribeSchema);
                          Task.Delay(1000);

                      }
                  }
                  catch (Exception ex)
                  {
                      log.Error("The following exception occured inside OnClose: " + ex.Message);
                  }
              };
               ws.Connect();
               ws.Send(subscribeSchema);
           });
        }
        private void WebsocketOnTimedEvent(object sender, ElapsedEventArgs e)
        {
            if (ws != null)
            {
                ws.Close();
            }
        }
        private static async Task SendToKafka(String stringData, String topic, long rdmNumber)
        {
            String[] topicSplit = topic.Split('-');

            if (topicSplit[2].ToLower().Equals("order"))
            {
                await coinbaseObj.createOrderBookJson(stringData, topic, rdmNumber);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await coinbaseObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await coinbaseObj.createTickerJson(stringData, topic);
            }
        }
    }
}
