using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Timers;
using WebSocketSharp;

namespace HitBtc
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Hitbtc hitbtcObj = new Hitbtc();
        private static WebSocket ws = null;
        public async Task getTickerData(String uri, String topicName)
        {
            while (true)
            {
                try
                {
                    IRestResponse response = null;
                    var client = new RestClient("https://api.hitbtc.com/");
                    var request = new RestRequest(uri, Method.GET);
                    response = client.Execute(request);
                    if ((response != null) && (response.Content.StartsWith("{") && response.Content.EndsWith("}")))
                    {
                        String receivedData = response.Content;
                        Dictionary<string, object> values = JsonConvert.DeserializeObject<Dictionary<string, object>>(receivedData);
                        if (values.ContainsKey("ask") && values.ContainsKey("bid"))
                        {
                            await SendToKafka(receivedData, topicName);
                        }
                    }
                    else
                    {
                        log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                    }
                }
                catch (Exception e)
                {
                    log.Info("Exception occured while: " + topicName);
                    log.Info("The Message of the exception: " + e.Message);
                    log.Error("The Source of the exception: " + e.Source);
                    log.Error("The StackTrace of the exception: " + e.StackTrace);
                }
            }
        }

        public static async Task getOrderAndTradeData(String socketAddress, String topicName)
        {
            await Task.Run(() =>
            {
                ws = new WebSocket(socketAddress);


                ws.OnOpen += (sender, e) =>
                {
                    log.Info(ws.ReadyState);
                    log.Info("Websocket Connected for: " + topicName);

                };

                ws.OnMessage += async (sender, e) =>
                {
                    String receivedData = e.Data;
                   
                    if ((receivedData.StartsWith("{") && receivedData.EndsWith("}")))
                    {
                        Dictionary<string, JObject> dictObj = JsonConvert.DeserializeObject<Dictionary<string, JObject>>(receivedData);
                        var data = JsonConvert.DeserializeObject<JObject>(receivedData);
                        if (dictObj.ContainsKey("MarketDataSnapshotFullRefresh"))
                        {
                            String symbol = data["MarketDataSnapshotFullRefresh"]["symbol"].ToString();
                            var currencies = topicName.Split('-');
                            if (symbol.Equals(currencies[0]))
                            {
                                //log.Info(receivedData);
                                 await SendToKafka(receivedData, "Hitbtc-" + currencies[0] + "-Order");
                            }
                            else if (symbol.Equals(currencies[1]))
                            {
                                // await SendToKafka(receivedData, "Hitbtc-" + currencies[1] + "-Order");
                            }
                            else if (symbol.Equals(currencies[2]))
                            {
                                // await SendToKafka(receivedData, "Hitbtc-" + currencies[2] + "-Order");
                            }
                            else if (symbol.Equals(currencies[3]))
                            {
                                // await SendToKafka(receivedData, "Hitbtc-" + currencies[3] + "-Order");
                            }
                        }
                        else if (dictObj.ContainsKey("MarketDataIncrementalRefresh"))
                        {
                            String symbol = data["MarketDataIncrementalRefresh"]["symbol"].ToString();
                            var currencies = topicName.Split('-');
                            if (symbol.Equals(currencies[0]))
                            {
                                //log.Info(receivedData);
                                await SendToKafka(receivedData, "Hitbtc-" + currencies[0] + "-Order");
                                await SendToKafka(receivedData, "Hitbtc-" + currencies[0] + "-Trade");
                            }
                            else if (symbol.Equals(currencies[1]))
                            {
                                //await SendToKafka(receivedData, "Hitbtc-" + currencies[1] + "-Order");
                                //await SendToKafka(receivedData, "Hitbtc-" + currencies[1] + "-Trade");
                            }
                            else if (symbol.Equals(currencies[2]))
                            {
                                //await SendToKafka(receivedData, "Hitbtc-" + currencies[2] + "-Order");
                                // await SendToKafka(receivedData, "Hitbtc-" + currencies[2] + "-Trade");
                            }
                            else if (symbol.Equals(currencies[3]))
                            {
                                //await SendToKafka(receivedData, "Hitbtc-" + currencies[3] + "-Order");
                                //await SendToKafka(receivedData, "Hitbtc-" + currencies[3] + "-Trade");
                            }
                        }
                    }
                };

                ws.OnError += (sender, e) =>
               {
                   log.Error("Error occured: " + e.Exception + " , Exception message:    " + e.Exception.Message );
                };
                ws.OnClose += (sender, e) =>
                {
                    log.Info("Websocket getting closed for the following topic : " + topicName + "\n");
                    try
                    {
                        while (!(ws.ReadyState.ToString().Equals("Open")))
                        {
                            log.Info("Trying to reconnect" + "\n");
                            log.Info(ws.ReadyState);
                            ws.Connect();
                      
                            Task.Delay(1000);

                        }
                    }
                    catch (Exception ex)
                    {
                        log.Error("The following exception occured inside OnClose: " + ex.Message);
                    }
                };
                ws.Connect();
            });
        }

        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');

            if (topicSplit[2].ToLower().Equals("order"))
            {
                await hitbtcObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await hitbtcObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await hitbtcObj.createTickerJson(stringData, topic);
            }
        }
    }
}
