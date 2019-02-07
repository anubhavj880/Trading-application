using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace Coinbase_ExchangeConnection_Client
{
    class Coinbase
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        public async Task createOrderBookJson(String orderBookData, String topic, long rdmNumber)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String[] topicSplit = topic.Split('-');
                JObject jsonData = JsonConvert.DeserializeObject<JObject>(orderBookData);
                IDictionary<string, JToken> dictionary = jsonData;

                if (dictionary.ContainsKey("bids") && dictionary.ContainsKey("asks"))
                {
                    long snapSeqNo = jsonData["sequence"].ToObject<long>();
                    JArray snapAskArray = jsonData["asks"].ToObject<JArray>();
                    JArray snapBidArray = jsonData["bids"].ToObject<JArray>();
                    JArray bidArray = new JArray();
                    JArray askArray = new JArray();
                    int askCount = 0;
                    int bidCount = 0;
                    foreach (var askjson in snapAskArray)
                    {
                        askCount += 1;
                        if (askCount > 500)
                        {
                            break;
                        }
                        JObject askobj = JObject.FromObject(new
                        {
                            UpdateType = "Snapshot",
                            ExchangeName = "COINBASE",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = "asks",
                            OrderId = askjson[2].ToString(),
                            Price = askjson[0].ToObject<Double>(),
                            Quantity = askjson[1].ToObject<Double>()
                        });
                        askArray.Add(askobj);
                    }
                    foreach (var bidjson in snapBidArray)
                    {
                        bidCount += 1;
                        if (bidCount > 500)
                        {
                            break;
                        }
                        JObject bidobj = JObject.FromObject(new
                        {
                            UpdateType = "Snapshot",
                            ExchangeName = "COINBASE",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = "bids",
                            OrderId = bidjson[2].ToString(),
                            Price = bidjson[0].ToObject<Double>(),
                            Quantity = bidjson[1].ToObject<Double>()
                        });
                        bidArray.Add(bidobj);
                    }
                    JObject snapjsonObj = JObject.FromObject(new
                    {
                        MachineTime = machineTime,
                        Bids = bidArray,
                        Asks = askArray,
                    });
                    JObject snapshotjson = JObject.FromObject(new
                    {
                        TimeStamp = timeStamp,
                        SequenceNumber = snapSeqNo,
                        RandomNumber = rdmNumber,
                        Snapshot = snapjsonObj,
                    });
                    try
                    {
                        await producer.ProduceAsync(topic, null, snapshotjson.ToString(Formatting.None));
                    }
                    catch (Exception ex)
                    {
                        log.Info(ex.Data + "\n");
                        log.Info(ex.Message);
                    }
                    log.Info(snapshotjson.ToString(Formatting.None));
                }
                else if (dictionary.ContainsKey("type"))
                {
                    String orderside = "";
                    Double quantity = 0.0;
                    String orderId = "";
                    Double price = 0.0;
                    if (dictionary.ContainsKey("maker_order_id"))
                    {
                        orderId = jsonData["maker_order_id"].ToString();
                    }
                    else
                    {
                        orderId = jsonData["order_id"].ToString();
                    }
                    if (jsonData["side"].ToString().Equals("buy"))
                    {
                        orderside = "bids";
                    }
                    else
                    {
                        orderside = "asks";
                    }
                    if (dictionary.ContainsKey("remaining_size"))
                    {
                        quantity = jsonData["remaining_size"].ToObject<Double>();
                    }
                    else if (dictionary.ContainsKey("size"))
                    {
                        quantity = jsonData["size"].ToObject<Double>();
                    }
                    else if (dictionary.ContainsKey("new_size"))
                    {
                        quantity = jsonData["new_size"].ToObject<Double>();
                    }
                    if (dictionary.ContainsKey("price"))
                    {
                        price = jsonData["price"].ToObject<Double>();
                    }

                    JObject orderobj = JObject.FromObject(new
                    {
                        UpdateType = jsonData["type"],
                        ExchangeName = "COINBASE",
                        CurrencyPair = topicSplit[1],
                        MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        OrderId = orderId,
                        OrderSide = orderside,
                        Price = price,
                        Quantity = quantity
                    });

                    JObject updateObj = JObject.FromObject(new
                    {
                        SequenceNumber = jsonData["sequence"].ToObject<long>(),
                        RandomNumber = rdmNumber,
                        TimeStamp = timeStamp,
                        Updates = orderobj
                    });
                    try
                    {
                        await producer.ProduceAsync(topic, null, updateObj.ToString(Formatting.None));

                    }
                    catch (Exception ex)
                    {
                        log.Info(ex.Data + "\n");
                        log.Info(ex.Message);
                    }
                    log.Info(updateObj.ToString(Formatting.None));
                }
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
        public async Task createTradeBookJson(String tradeData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                JObject jsonData = JsonConvert.DeserializeObject<JObject>(tradeData);
                String[] topicSplit = topic.Split('-');
                JObject tradeObj = JObject.FromObject(new
                {
                    ExchangeName = "COINBASE",
                    CurrencyPair = topicSplit[1],
                    TradeTime = DateTime.ParseExact(jsonData["time"].ToString(), "yyyy-MM-dd hh:mm:ss tt", CultureInfo.InvariantCulture).AddSeconds(19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeSide = jsonData["side"].ToString(),
                    TradeId = jsonData["trade_id"].ToString(),
                    Price = jsonData["price"].ToObject<Double>(),
                    Volume = jsonData["size"].ToObject<Double>(),
                    TotalBase = (jsonData["price"].ToObject<Double>()) * (jsonData["size"].ToObject<Double>())
                });

                JObject tradeBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TradeBook = tradeObj,

                });
                log.Info(tradeBook.ToString(Formatting.None));
                await producer.ProduceAsync(topic, null, tradeBook.ToString(Formatting.None));
            }

            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
        public async Task createTickerJson(String tickerData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                JObject jsonData = JsonConvert.DeserializeObject<JObject>(tickerData);
                String[] topicSplit = topic.Split('-');
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "COINBASE",
                    CurrencyPair = topicSplit[1],
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    BidPrice = jsonData["bid"].ToObject<Double>(),
                    AskPrice = jsonData["ask"].ToObject<Double>(),
                    LastPrice = jsonData["price"].ToObject<Double>()
                });
                JObject tickerBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TickerBook = tickerObj,

                });
                log.Info(tickerBook.ToString(Formatting.None));
                await producer.ProduceAsync(topic, null, tickerBook.ToString(Formatting.None));
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
    }
}
