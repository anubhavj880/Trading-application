using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace Bittrex_ExchangeClient
{
    class Bittrex
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        private long bitrexTradeId = 0L;

        public async Task createOrderBookJson(String orderBookData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                IDictionary<string, JToken> jsondata = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String[] topicSplit = topic.Split('-');

                List<JObject> snapBidList = jsondata["result"]["buy"].ToObject<List<JObject>>();
                List<JObject> snapAskList = jsondata["result"]["sell"].ToObject<List<JObject>>();
                JArray bidArray = new JArray();
                JArray askArray = new JArray();
                int bidCount = 0;
                int askCount = 0;
                foreach (var bid in snapBidList)
                {
                    bidCount += 1;
                    if (bidCount == 50)
                    {
                        break;
                    }
                    if ((bid["Rate"].ToObject<Double>() != 0.0) && (bid["Quantity"].ToObject<Double>() != 0.0))
                    {
                        JObject bidObj = JObject.FromObject(new
                        {
                            ExchangeName = "BITTREX",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = "bids",
                            Price = bid["Rate"].ToObject<Double>(),
                            Quantity = bid["Quantity"].ToObject<Double>()
                        });
                        bidArray.Add(bidObj);
                    }

                }
                foreach (var ask in snapAskList)
                {
                    askCount += 1;
                    if (askCount == 50)
                    {
                        break;
                    }
                    if ((ask["Rate"].ToObject<Double>() != 0.0) && (ask["Quantity"].ToObject<Double>() != 0.0))
                    {
                        JObject askObj = JObject.FromObject(new
                        {
                            ExchangeName = "BITTREX",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = "asks",
                            Price = ask["Rate"].ToObject<Double>(),
                            Quantity = ask["Quantity"].ToObject<Double>()
                        });
                        askArray.Add(askObj);
                    }

                }

                JObject snapshotjson = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    Snapshot = JObject.FromObject(new
                    {
                        MachineTime = machineTime,
                        Bids = bidArray,
                        Asks = askArray,
                    }),
                });

                await producer.ProduceAsync(topic, null, snapshotjson.ToString(Formatting.None));
                log.Info(snapshotjson.ToString(Formatting.None));
                orderBookData = null;
                jsondata = null;
            }
            catch (Exception ex)
            {

                log.Info(ex.Message);
            }



        }
        public async Task createTradeBookJson(String tradeBookData, String topic)
        {
            try
            {
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                IDictionary<string, JToken> jsondata = JsonConvert.DeserializeObject<JObject>(tradeBookData);
                String[] topicSplit = topic.Split('-');

                JArray tradeArray = jsondata["result"].ToObject<JArray>();
                if (jsondata["result"][0]["Id"].ToObject<long>() > bitrexTradeId)
                {
                    bitrexTradeId = jsondata["result"][0]["Id"].ToObject<long>();

                    foreach (JObject trade in tradeArray)
                    {

                        String tradeSideString = "";
                        if ((trade["OrderType"].ToString()).Equals("BUY"))
                        {
                            tradeSideString = "buy";
                        }
                        else if ((trade["OrderType"].ToString()).Equals("SELL"))
                        {
                            tradeSideString = "sell";
                        }

                        JObject tradeBook = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            TradeBook = JObject.FromObject(new
                            {
                                ExchangeName = "BITTREX",
                                CurrencyPair = topicSplit[1],
                                MachineTime = machineTime,
                                TradeTime = DateTime.ParseExact(trade["TimeStamp"].ToString(), "yyyy-MM-dd hh:mm:ss tt", CultureInfo.InvariantCulture).AddSeconds(19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                TradeSide = tradeSideString,
                                Price = Convert.ToDouble(trade["Price"]),
                                Volume = Convert.ToDouble(trade["Quantity"]),
                                TotalBase = Convert.ToDouble(trade["Total"])

                            }),

                        });

                        await producer.ProduceAsync(topic, null, tradeBook.ToString(Formatting.None));
                        log.Info(tradeBook.ToString(Formatting.None));
                    }
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
                IDictionary<string, JToken> jsondata = JsonConvert.DeserializeObject<JObject>(tickerData);
                String[] topicSplit = topic.Split('-');

                JObject tickerBook = JObject.FromObject(new
                {
                    TimeStamp = timeStamp,
                    TickerBook = JObject.FromObject(new
                    {
                        ExchangeName = "BITTREX",
                        CurrencyPair = topicSplit[1],
                        MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        BidPrice = Convert.ToDouble(jsondata["result"]["Bid"]),
                        AskPrice = Convert.ToDouble(jsondata["result"]["Ask"]),
                        LastPrice = Convert.ToDouble(jsondata["result"]["Last"])
                    }),

                });
                log.Info(tickerBook.ToString(Formatting.None));
                await producer.ProduceAsync(topic, null, tickerBook.ToString(Formatting.None));
                tickerBook = null;
                tickerData = null;
                jsondata = null;
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }

        }

    }
}