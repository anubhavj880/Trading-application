using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Yobit_ExchangeConnection_Client
{
    class Yobit
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
                var jobject = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String[] topicSplit = topic.Split('-');
                var bidAskDict = jobject[topicSplit[1].ToLower()].ToObject<JObject>();
                JArray snapBidArray = bidAskDict["bids"].ToObject<JArray>();
                JArray snapAskArray = bidAskDict["asks"].ToObject<JArray>();
                JArray bidArray = new JArray();
                JArray askArray = new JArray();
                String currency = null;
                if (topicSplit[1].Equals("ETH_BTC"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("LTC_BTC"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("DASH_BTC"))
                {
                    currency = "DSHBTC";
                }
                foreach (var bid in snapBidArray)
                {
                    if ((bid[0].ToObject<Double>() != 0.0) && (bid[1].ToObject<Double>() != 0.0))
                    {
                        JObject bidObj = JObject.FromObject(new
                        {
                            ExchangeName = "YOBIT",
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
                            ExchangeName = "YOBIT",
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

        public async Task createTickerJson(string tickerData, string topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                var jsondata = JsonConvert.DeserializeObject<JObject>(tickerData);

                String[] topicSplit = topic.Split('-');
                var ticData = jsondata[topicSplit[1].ToLower()].ToObject<JObject>();
                String currency = null;
                if (topicSplit[1].Equals("ETH_BTC"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("LTC_BTC"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("DASH_BTC"))
                {
                    currency = "DSHBTC";
                }
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "YOBIT",
                    CurrencyPair = currency,
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    BidPrice = ticData["buy"].ToObject<Double>(),
                    AskPrice = ticData["sell"].ToObject<Double>(),
                    LastPrice = ticData["last"].ToObject<Double>()

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

        public async Task createTradeBookJson(String tradeBookData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                var jsonData = JsonConvert.DeserializeObject<JObject>(tradeBookData);
                String[] topicSplit = topic.Split('-');

                String currency = "";
                if (topicSplit[1].Equals("ETH_BTC"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("LTC_BTC"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("DASH_BTC"))
                {
                    currency = "DSHBTC";
                }
                String tradeSide = "";
                if (jsonData["type"].ToString().Equals("bid"))
                {
                    tradeSide = "buy";
                }
                else
                {
                    tradeSide = "sell";
                }
                JObject tradeObj = JObject.FromObject(new
                {
                    ExchangeName = "YOBIT",
                    CurrencyPair = currency,
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeTime = new DateTime(1970, 1, 1).AddSeconds(Convert.ToDouble(jsonData["timestamp"]) + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeSide = tradeSide,
                    TradeId = jsonData["tid"].ToString(),
                    Price = jsonData["price"].ToObject<Double>(),
                    Volume = jsonData["amount"].ToObject<Double>(),
                    TotalBase = (Convert.ToDouble(jsonData["price"]) * Convert.ToDouble(jsonData["amount"]))
                });
                JObject tradeBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TradeBook = tradeObj,

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
