using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Gatecoin_ExchangeConnection_client
{
    class Gatecoin
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
                IDictionary<string, JToken> jsonOrders = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String[] topicSplit = topic.Split('-');
                if (jsonOrders.ContainsKey("asks") && jsonOrders.ContainsKey("bids"))
                {
                    JArray snapBidArray = jsonOrders["bids"].ToObject<JArray>();
                    JArray snapAskArray = jsonOrders["asks"].ToObject<JArray>();
                    JArray bidArray = new JArray();
                    JArray askArray = new JArray();
                    foreach (var bid in snapBidArray)
                    {
                        JObject bidObj = JObject.FromObject(new
                        {
                            ExchangeName = "GATECOIN",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = "bids",
                            Price = bid["price"].ToObject<Double>(),
                            Quantity = bid["volume"].ToObject<Double>()
                        });
                        bidArray.Add(bidObj);
                    }
                    foreach (var ask in snapAskArray)
                    {
                        JObject askObj = JObject.FromObject(new
                        {
                            ExchangeName = "GATECOIN",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = "asks",
                            Price = ask["price"].ToObject<Double>(),
                            Quantity = ask["volume"].ToObject<Double>()
                        });
                        askArray.Add(askObj);

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
                IDictionary<string, JToken> tickers = JsonConvert.DeserializeObject<JObject>(tickerData);
                if (tickers.ContainsKey("tickers") && tickers["tickers"].ToObject<JArray>().Count > 0)
                {
                    String[] topicSplit = topic.Split('-');
                    var tickerArray = tickers["tickers"].ToObject<JArray>();
                    foreach (var ticker in tickerArray)
                    {
                        if (ticker["currencyPair"].ToString().Equals(topicSplit[1]))
                        {
                            JObject tickerObj = JObject.FromObject(new
                            {
                                ExchangeName = "GATECOIN",
                                CurrencyPair = ticker["currencyPair"].ToString(),
                                MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                BidPrice = ticker["bid"].ToObject<Double>(),
                                BidSize = ticker["bidQ"].ToObject<Double>(),
                                AskPrice = ticker["ask"].ToObject<Double>(),
                                AskSize = ticker["askQ"].ToObject<Double>(),
                                LastPrice = ticker["last"].ToObject<Double>()

                            });
                            JObject tickerBook = JObject.FromObject(new
                            {
                                TimeStamp = timeStamp,
                                TickerBook = tickerObj,

                            });
                            await producer.ProduceAsync(topic, null, tickerBook.ToString(Formatting.None));
                            log.Info(tickerBook.ToString(Formatting.None));
                        }
                    }


                }

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
                String tradeSide = "";
                if (jsonData["way"].ToString().Equals("bid"))
                {
                    tradeSide = "buy";
                }
                else
                {
                    tradeSide = "sell";
                }
                JObject tradeObj = JObject.FromObject(new
                {
                    ExchangeName = "GATECOIN",
                    CurrencyPair = jsonData["currencyPair"].ToString(),
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeTime = new DateTime(1970, 1, 1).AddSeconds(Convert.ToDouble(jsonData["transactionTime"]) + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeSide = tradeSide,
                    TradeId = jsonData["transactionId"].ToString(),
                    Price = jsonData["price"].ToObject<Double>(),
                    Volume = jsonData["quantity"].ToObject<Double>(),
                    TotalBase = (Convert.ToDouble(jsonData["price"]) * Convert.ToDouble(jsonData["quantity"]))
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
