using RestSharp;
using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using WebSocketSharp;

namespace Gemini_ExchangeClient
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Gemini geminiObj = new Gemini();
        private String uri = "";
        private String topicName = "";
        private WebSocket ws = null;
        private enum SslProtocolsHack
        {
            Tls = 192,
            Tls11 = 768,
            Tls12 = 3072
        }
        public async Task SendHttpRequest(String uri, String topicName)
        {
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
                        log.Info("Exception occured while HttpRequest: " + topicName);
                        log.Error("The Source of the exception: " + e.Source);
                        log.Error("The StackTrace of the exception: " + e.StackTrace);
                    }


                    if ((response != null) && response.IsSuccessStatusCode)
                    {
                        String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());
                        await SendToKafka(exchangeData, topicName);
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

        /*   public async Task webSocketConnect(String websocketAddress, String topicName)
           {

               await Task.Run(() =>
               {
                   this.websocketAddress = websocketAddress;
                   ws = new WebSocket(websocketAddress);
                   System.Timers.Timer aTimer = new System.Timers.Timer();
                   aTimer.Elapsed += new ElapsedEventHandler(websocketOnTimedEvent);
                   aTimer.Interval = 900000;
                   aTimer.Enabled = true;
                   ws.Opened += (sender, e) =>
                   {
                       log.Info(ws.EnableAutoSendPing);
                       log.Info("Websocket Connected for: " + "ETHBTC");
                   };
                   ws.MessageReceived += async (sender, e) =>
                  {
                      //log.Info(e.Message);
                      await SendToKafka(e.Message, topicName);
                  };

                   ws.Error += (sender, e) =>
                   {
                       log.Error("Error occured: " + e.Exception + " , Exception message:    " + e.Exception.Message + ",  InnerException:   " + e.Exception.InnerException + ",  Exception Source:   " + e.Exception.Source + ",  Method throws the exception: " + e.Exception.TargetSite);


                   };
                   ws.Closed += (sender, e) =>
                   {
                       log.Info("Websocket state : " + ws.State.ToString());
                       try
                       {
                           if (!(ws.State.ToString().Equals("Open")) && !(ws.State.ToString().Equals("Connecting")))
                           {
                               ws.Open();
                               log.Info(ws.State.ToString());
                               Task.Delay(1000);

                           }
                           log.Info("State after coming out of the while loop:  " + ws.State.ToString());
                       }
                       catch(Exception ex)
                       {
                           log.Info("The following exception occure in Closed event method: " + ex.Message);
                       }
                   };
                   ws.Open();
               });
           }*/
        public async Task webSocketConnect(String uri, String topicName)
        {

            await Task.Run(() =>
            {
                ws = new WebSocket(uri);
                ws.EmitOnPing = true;
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(websocketOnTimedEvent);
                aTimer.Interval = 900000;
                aTimer.Enabled = true;
                ws.OnOpen += (sender, e) =>
                {
                    log.Info(ws.ReadyState);
                    log.Info("Websocket Connected for: " + topicName);

                };

                ws.OnMessage += async (sender, e) =>
                {
                    //log.Info(e.Data );
                    await SendToKafka(e.Data, topicName);
                };

                ws.OnError += (sender, e) =>
                {
                    log.Error("Error occured in the : " + topicName + " with the message " + e.Message);


                };
                ws.OnClose += (sender, e) =>
                {
                    log.Info("Websocket getting closed for the following topic : " + topicName + "\n");
                    try
                    {
                        while (!(ws.ReadyState.ToString().Equals("Open")))
                        {
                            var sslProtocolHack = (System.Security.Authentication.SslProtocols)(SslProtocolsHack.Tls12 | SslProtocolsHack.Tls11 | SslProtocolsHack.Tls);
                            //TlsHandshakeFailure
                            if (e.Code == 1015 && ws.SslConfiguration.EnabledSslProtocols != sslProtocolHack)
                            {
                                ws.SslConfiguration.EnabledSslProtocols = sslProtocolHack;
                                ws.Connect();
                            }
                            else
                            {
                                log.Info("Trying to reconnect" + "\n");
                                log.Info(ws.ReadyState);
                                ws.SslConfiguration.EnabledSslProtocols = sslProtocolHack;
                                ws.Connect();
                            }


                            Task.Delay(1000);

                        }
                    }
                    catch (Exception ex)
                    {
                        log.Error("The following exception occured inside OnClose: " + ex.Message);
                        Task.Delay(10000);
                    }
                };
                ws.Connect();


            });
        }
        private void websocketOnTimedEvent(object sender, ElapsedEventArgs e)
        {
            if (ws != null && (ws.ReadyState.ToString().Equals("Open")))
            {
                ws.Close();
            }

        }
        private async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');
            if (topicSplit.Last().ToLower().Equals("order"))
            {
                await geminiObj.createOrderBookJson(stringData, topic);
            }

            else if (topicSplit.Last().ToLower().Equals("ticker"))
            {
                await geminiObj.createTickerJson(stringData, topic);
            }
        }
    }
}
