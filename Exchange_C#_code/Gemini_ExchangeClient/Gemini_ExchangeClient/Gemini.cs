using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Gemini_ExchangeClient
{
    class Gemini
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));

        public async Task createOrderBookJson(String orderBookData, String topic)
        {
            try
            {
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                IDictionary<string, JToken> orderDict = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String[] topicSplit = topic.Split('-');
                if (orderDict.ContainsKey("events") && (orderDict.ContainsKey("eventId")))
                {
                    JArray eventsArray = orderDict["events"].ToObject<JArray>();
                    long eventId = orderDict["eventId"].ToObject<long>();
                    IDictionary<string, JToken> firstevent = eventsArray[0].ToObject<JObject>();
                    if (eventsArray.Count > 0 && firstevent.ContainsKey("reason") && firstevent["reason"].ToString().Equals("initial"))
                    {
                        JArray bidArray = new JArray();
                        JArray askArray = new JArray();
                        foreach (JObject order in eventsArray)
                        {
                            if (order["side"].ToString().Equals("bid"))
                            {
                                JObject bidObj = JObject.FromObject(new
                                {
                                    ExchangeName = "GEMINI",
                                    CurrencyPair = topicSplit[1],
                                    MachineTime = machineTime,
                                    OrderSide = "bids",
                                    Price = order["price"].ToObject<Double>(),
                                    Quantity = order["remaining"].ToObject<Double>(),
                                    UpdateType = order["reason"]
                                });
                                bidArray.Add(bidObj);
                            }
                            else if (order["side"].ToString().Equals("ask"))
                            {
                                JObject askObj = JObject.FromObject(new
                                {
                                    ExchangeName = "GEMINI",
                                    CurrencyPair = topicSplit[1],
                                    MachineTime = machineTime,
                                    OrderSide = "asks",
                                    Price = order["price"].ToObject<Double>(),
                                    Quantity = order["remaining"].ToObject<Double>(),
                                    UpdateType = order["reason"]
                                });
                                askArray.Add(askObj);
                            }
                        }
                        JObject orderbookSnapShot = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            EventId = eventId,
                            Snapshot = JObject.FromObject(new
                            {
                                MachineTime = machineTime,
                                Bids = bidArray,
                                Asks = askArray
                            })
                        });
                        try
                        {
                            var deliveryReport = producer.ProduceAsync(topic, null, orderbookSnapShot.ToString(Formatting.None));
                            await deliveryReport.ContinueWith(task =>
                            {
                                log.Info($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                            });
                        }
                        catch (Exception ex)
                        {
                            log.Info(ex.Data + "\n");
                            log.Info(ex.Message);
                        }
                        log.Info(orderbookSnapShot.ToString(Formatting.None));
                    }
                    else if (eventsArray.Count == 2)
                    {

                        JObject orderbookUpdate = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            EventId = eventId,
                            Updates = JObject.FromObject(new
                            {
                                ExchangeName = "GEMINI",
                                CurrencyPair = topicSplit[1],
                                MachineTime = machineTime,
                                OrderSide = eventsArray[1]["side"] + "s",
                                Price = eventsArray[1]["price"].ToObject<Double>(),
                                Quantity = eventsArray[1]["remaining"].ToObject<Double>(),
                                UpdateType = eventsArray[1]["reason"]
                            })
                        });
                        try
                        {
                            await producer.ProduceAsync(topic, null, orderbookUpdate.ToString(Formatting.None));

                        }
                        catch (Exception ex)
                        {
                            log.Info(ex.Data + "\n");
                            log.Info(ex.Message);
                        }
                        log.Info(orderbookUpdate.ToString(Formatting.None));


                        await createTradeBookJson(eventsArray[0].ToObject<JObject>(), topicSplit[0] + "-" + topicSplit[1] + "-Trade");

                    }
                    else if (eventsArray.Count == 1)
                    {

                        JObject orderbookUpdate = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            EventId = eventId,
                            Updates = JObject.FromObject(new
                            {
                                ExchangeName = "GEMINI",
                                CurrencyPair = topicSplit[1],
                                MachineTime = machineTime,
                                OrderSide = eventsArray[0]["side"] + "s",
                                Price = eventsArray[0]["price"].ToObject<Double>(),
                                Quantity = eventsArray[0]["remaining"].ToObject<Double>(),
                                UpdateType = eventsArray[0]["reason"]
                            })
                        });
                        try
                        {
                            await producer.ProduceAsync(topic, null, orderbookUpdate.ToString(Formatting.None));

                        }
                        catch (Exception ex)
                        {
                            log.Info(ex.Data + "\n");
                            log.Info(ex.Message);
                        }
                        log.Info(orderbookUpdate.ToString(Formatting.None));

                    }
                }
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }

        public async Task createTickerJson(String tradeString, String topic)
        {
            try
            {
                var jsonData = JsonConvert.DeserializeObject<JObject>(tradeString);
                String[] topicSplit = topic.Split('-');
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "GEMINI",
                    CurrencyPair = topicSplit[1],
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    BidPrice = jsonData["bid"].ToObject<Double>(),
                    AskPrice = jsonData["ask"].ToObject<Double>(),
                    LastPrice = jsonData["last"].ToObject<Double>()
                });
                JObject tickerBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TickerBook = tickerObj,

                });


                await producer.ProduceAsync(topic, null, tickerBook.ToString(Formatting.None));
                log.Info("Gemini ETHBTC ticker data:  " + tickerBook.ToString(Formatting.None));
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }

        public async Task createTradeBookJson(JObject tradeObj, String topic)
        {
            try
            {
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String[] topicSplit = topic.Split('-');
                String tradeSideString = "";
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                if (tradeObj["makerSide"].ToString().Equals("ask"))
                {
                    tradeSideString = "sell";
                }
                else
                {
                    tradeSideString = "buy";
                }

                JObject tradeBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TradeBook = JObject.FromObject(new
                    {
                        ExchangeName = "GEMINI",
                        CurrencyPair = topicSplit[1],
                        TradeId = tradeObj["tid"].ToString(),
                        TradeTime = machineTime,
                        MachineTime = machineTime,
                        TradeSide = tradeSideString,
                        Price = tradeObj["price"].ToObject<Double>(),
                        Volume = tradeObj["amount"].ToObject<Double>(),
                        TotalBase = (tradeObj["price"].ToObject<Double>()) * (tradeObj["amount"].ToObject<Double>())

                    })

                });
                await producer.ProduceAsync(topic, null, tradeBook.ToString(Formatting.None));
                log.Info(tradeBook.ToString(Formatting.None));
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
    }
}
