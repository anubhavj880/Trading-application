using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Kraken_ExchangeConnection_Client
{
    class Kraken
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        public async Task createOrderBookJson(String orderBookData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");

                var jsondata = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String[] topicSplit = topic.Split('-');
                var bidAskDict = jsondata["result"][topicSplit[1]].ToObject<JObject>();
                JArray snapBidArray = bidAskDict["bids"].ToObject<JArray>();
                JArray snapAskArray = bidAskDict["asks"].ToObject<JArray>();
                JArray bidArray = new JArray();
                JArray askArray = new JArray();
                String currency = "";
                if (topicSplit[1].Equals("XETHXXBT"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("XLTCXXBT"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("XXMRXXBT"))
                {
                    currency = "XMRBTC";
                }
                else if (topicSplit[1].Equals("DASHXBT"))
                {
                    currency = "DSHBTC";
                }
                else if (topicSplit[1].Equals("XXRPXXBT"))
                {
                    currency = "XRPBTC";
                }
                foreach (var bid in snapBidArray)
                {
                    if ((bid[0].ToObject<Double>() != 0.0) && (bid[1].ToObject<Double>() != 0.0))
                    {
                        JObject bidObj = JObject.FromObject(new
                        {
                            ExchangeName = "KRAKEN",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            OrderSide = "bids",
                            Price = bid[0].ToObject<Double>(),
                            Quantity = bid[1].ToObject<Double>()
                        });
                        bidArray.Add(bidObj);
                    }
                }
                foreach (var ask in snapAskArray)
                {
                    if ((ask[0].ToObject<Double>() != 0.0) && (ask[1].ToObject<Double>() != 0.0))
                    {
                        JObject askObj = JObject.FromObject(new
                        {
                            ExchangeName = "KRAKEN",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            OrderSide = "asks",
                            Price = ask[0].ToObject<Double>(),
                            Quantity = ask[1].ToObject<Double>()
                        });
                        askArray.Add(askObj);
                    }
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
                    Snapshot = snapjsonObj,
                });

                await producer.ProduceAsync(topic, null, snapshotjson.ToString(Formatting.None));
                log.Info(snapshotjson.ToString(Formatting.None));
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }

        }
        public async Task createTradeBookJson(String tradeBookData, String topic)
        {
            try
            {

                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                var jsondata = JsonConvert.DeserializeObject<JObject>(tradeBookData);
                String[] topicSplit = topic.Split('-');
                var listOfList = jsondata["result"][topicSplit[1]].ToObject<List<List<Object>>>();
                String currency = null;
                if (topicSplit[1].Equals("XETHXXBT"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("XLTCXXBT"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("XXMRXXBT"))
                {
                    currency = "XMRBTC";
                }
                else if (topicSplit[1].Equals("DASHXBT"))
                {
                    currency = "DSHBTC";
                }
                else if (topicSplit[1].Equals("XXRPXXBT"))
                {
                    currency = "XRPBTC";
                }
                foreach (var list in listOfList)
                {
                    String tradeSideString = "";
                    if ((list[3].ToString()).Equals("b"))
                    {
                        tradeSideString = "buy";
                    }
                    else if ((list[3].ToString()).Equals("s"))
                    {
                        tradeSideString = "sell";
                    }
                    JObject tradeObj = JObject.FromObject(new
                    {
                        ExchangeName = "KRAKEN",
                        CurrencyPair = currency,
                        MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        TradeTime = new DateTime(1970, 1, 1).AddSeconds(Convert.ToDouble(list[2]) + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        TradeSide = tradeSideString,
                        Price = Convert.ToDouble(list[0]),
                        Volume = Convert.ToDouble(list[1]),
                        TotalBase = (Convert.ToDouble(list[0]) * Convert.ToDouble(list[1]))

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
        public async Task createTickerJson(String tickerData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                var jsondata = JsonConvert.DeserializeObject<JObject>(tickerData);
                String[] topicSplit = topic.Split('-');
                var askList = jsondata["result"][topicSplit[1]]["a"].ToObject<List<Object>>();
                var bidList = jsondata["result"][topicSplit[1]]["b"].ToObject<List<Object>>();
                var priceList = jsondata["result"][topicSplit[1]]["c"].ToObject<List<Object>>();
                String currency = "";
                if (topicSplit[1].Equals("XETHXXBT"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("XLTCXXBT"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("XXMRXXBT"))
                {
                    currency = "XMRBTC";
                }
                else if (topicSplit[1].Equals("DASHXBT"))
                {
                    currency = "DSHBTC";
                }
                else if (topicSplit[1].Equals("XXRPXXBT"))
                {
                    currency = "XRPBTC";
                }
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "KRAKEN",
                    CurrencyPair = currency,
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    BidPrice = Convert.ToDouble(bidList[0]),
                    BidSize = Convert.ToDouble(bidList[2]),
                    AskPrice = Convert.ToDouble(askList[0]),
                    AskSize = Convert.ToDouble(askList[2]),
                    LastPrice = Convert.ToDouble(priceList[0])
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
    }
}
