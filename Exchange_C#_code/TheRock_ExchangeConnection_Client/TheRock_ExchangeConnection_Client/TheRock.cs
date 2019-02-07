using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace TheRock_ExchangeConnection_Client
{
    class TheRock
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));

        public async Task createTickerJson(String exchangeData, String topic)
        {
            try
            {
                var jsondata = JsonConvert.DeserializeObject<JObject>(exchangeData);
                String[] topicSplit = topic.Split('-');
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String currency = null;
                if (topicSplit[1].Equals("BTCXRP"))
                {
                    currency = "XRPBTC";
                }
                else
                {
                    currency = topicSplit[1];
                }
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "THEROCK",
                    CurrencyPair = currency,
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    BidPrice = jsondata["bid"].ToObject<Double>(),
                    AskPrice = jsondata["ask"].ToObject<Double>(),
                    LastPrice = jsondata["last"].ToObject<Double>()
                });
                JObject tickerBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TickerBook = tickerObj,

                });


                await producer.ProduceAsync(topic, null, tickerBook.ToString(Formatting.None));
                log.Info(tickerBook.ToString(Formatting.None));
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }


        }

        public async Task createTradeBookJson(String tradedata, String topic)
        {
            try
            {
                var jsondata = JsonConvert.DeserializeObject<JObject>(tradedata);
                String symbol = jsondata["symbol"].ToObject<String>();
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String[] topicSplit = topic.Split('-');
                if (symbol.Equals(topicSplit[1]))
                {

                    String currency = null;
                    if (symbol.Equals("BTCXRP"))
                    {
                        currency = "XRPBTC";
                    }
                    else
                    {
                        currency = symbol;
                    }
                    String tradeTime = jsondata["time"].ToString();
                    log.Info(tradeTime);
                    JObject tradeObj = JObject.FromObject(new
                    {
                        ExchangeName = "THEROCK",
                        CurrencyPair = currency,
                        TradeTime = jsondata["time"],
                        MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        TradeSide = jsondata["side"].ToString(),
                        Price = jsondata["value"].ToObject<Double>(),
                        Volume = jsondata["quantity"].ToObject<Double>(),
                        TotalBase = (jsondata["value"].ToObject<Double>()) * (jsondata["quantity"].ToObject<Double>())
                    });
                    JObject tradeBook = JObject.FromObject(new
                    {
                        TimeStamp = timeStamp,
                        TradeBook = tradeObj,

                    });

                    try
                    {
                        await producer.ProduceAsync(topic, null, tradeBook.ToString(Formatting.None));
                    }
                    catch (Exception ex)
                    {
                        log.Info(ex.Data + "\n");
                        log.Info(ex.Message);
                    }
                    log.Info(tradeBook.ToString(Formatting.None));
                    tradeBook = null;
                }
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }

        }

        public async Task createOrderBookJson(String Orderdata, String topic)
        {
            try
            {
                var jsondata = JsonConvert.DeserializeObject<JObject>(Orderdata);
                IDictionary<string, JToken> dictionary = jsondata;
                String[] topicSplit = topic.Split('-');
                if (dictionary.ContainsKey("asks") && dictionary.ContainsKey("bids"))
                {
                    String currency = null;
                    if (topicSplit[1].Equals("BTCXRP"))
                    {
                        currency = "XRPBTC";
                    }
                    else
                    {
                        currency = topicSplit[1];
                    }
                    JArray bidarray = jsondata["bids"].ToObject<JArray>();
                    JArray askarray = jsondata["asks"].ToObject<JArray>();
                    String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                    String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                    JArray snapshotBidarray = new JArray();
                    JArray snapshotAskarray = new JArray();
                    foreach (var bid in bidarray)
                    {
                        JObject obj = JObject.FromObject(new
                        {
                            ExchangeName = "THEROCK",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            OrderSide = "bids",
                            Price = bid["price"].ToObject<Double>(),
                            Quantity = bid["amount"].ToObject<Double>()
                        });
                        snapshotBidarray.Add(obj);
                    }
                    foreach (var ask in askarray)
                    {
                        JObject obj = JObject.FromObject(new
                        {
                            ExchangeName = "THEROCK",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            OrderSide = "asks",
                            Price = ask["price"].ToObject<Double>(),
                            Quantity = ask["amount"].ToObject<Double>()
                        });
                        snapshotAskarray.Add(obj);
                    }
                    JObject orderbookSnapShot = JObject.FromObject(new
                    {
                        TimeStamp = timeStamp,
                        Bids = snapshotBidarray,
                        Asks = snapshotAskarray
                    });
                    try
                    {
                        await producer.ProduceAsync(topic, null, orderbookSnapShot.ToString(Formatting.None));

                    }
                    catch (Exception ex)
                    {
                        log.Info(ex.Data + "\n");
                        log.Info(ex.Message);
                    }
                    log.Info(orderbookSnapShot.ToString(Formatting.None));

                }
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }

        }
        /*  public async Task createOrderBookJson(String Orderdata, String topic)
          {
              //log.Info("Entering to createOrderBookJson TheRock method: " + Thread.CurrentThread.ManagedThreadId + " - " + currencyPair);
              log.Info(Orderdata);
              var jsonData = JsonConvert.DeserializeObject<JObject>(Orderdata);
              IDictionary<string, JToken> dictionary = jsonData;
              String[] topicSplit = topic.Split('-');
              if (dictionary.ContainsKey("fund_id"))
              {
                  String currency = null;
                  if (topicSplit[1].Equals("BTCXRP"))
                  {
                      currency = "XRPBTC";
                  }
                  else
                  {
                      currency = topicSplit[1];
                  }
                  String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                  String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                  JArray askArray = jsonData.GetValue("asks").ToObject<JArray>();
                  JArray bidArray = jsonData.GetValue("bids").ToObject<JArray>();
                  JArray snapshotJarray = new JArray();
                  foreach (JObject snapshotbid in bidArray)
                  {
                      JObject obj = JObject.FromObject(new
                      {
                          ExchangeName = "THEROCK",
                          CurrencyPair = currency,
                          MachineTime = machineTime,
                          OrderSide = "bids",
                          Price = snapshotbid.GetValue("price").ToObject<Double>(),
                          Quantity = snapshotbid.GetValue("amount").ToObject<Double>(),
                          Depth = snapshotbid.GetValue("depth").ToObject<Double>()
                      });
                      snapshotJarray.Add(obj);
                  }
                  foreach (JObject snapshotask in askArray)
                  {
                      JObject obj = JObject.FromObject(new
                      {
                          ExchangeName = "THEROCK",
                          CurrencyPair = currency,
                          MachineTime = machineTime,
                          OrderSide = "asks",
                          Price = snapshotask.GetValue("price").ToObject<Double>(),
                          Quantity = snapshotask.GetValue("amount").ToObject<Double>(),
                          Depth = snapshotask.GetValue("depth").ToObject<Double>()
                      });
                      snapshotJarray.Add(obj);
                  }
                  JObject orderbookSnapShot = JObject.FromObject(new
                  {
                      TimeStamp = timeStamp,
                      Snapshot = snapshotJarray
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
              else if (dictionary.ContainsKey("side"))
              {
                  //log.Info(jsonData.ToString(Formatting.None));
                  String updateTime = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                  String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                  String currency = null;
                  if (topicSplit[1].Equals("BTCXRP"))
                  {
                      currency = "XRPBTC";
                  }
                  else
                  {
                      currency = topicSplit[1];
                  }
                  JObject updateObj = JObject.FromObject(new
                  {
                      ExchangeName = "THEROCK",
                      CurrencyPair = currency,
                      MachineTime = machineTime,
                      OrderSide = jsonData.GetValue("side") +"s",
                      Price = jsonData.GetValue("price").ToObject<Double>(),
                      Quantity = jsonData.GetValue("amount").ToObject<Double>()
                  });
                  JObject orderbookUpdates = JObject.FromObject(new
                  {
                      TimeStamp = updateTime,
                      Updates = updateObj
                  });
                  try
                  {

                       await producer.ProduceAsync(topic, null, orderbookUpdates.ToString(Formatting.None));
                  }
                  catch (Exception ex)
                  {
                      log.Info(ex.Data + "\n");
                      log.Info(ex.Message);
                  }
                  log.Info(orderbookUpdates.ToString(Formatting.None));
              }

          }*/
    }
}
