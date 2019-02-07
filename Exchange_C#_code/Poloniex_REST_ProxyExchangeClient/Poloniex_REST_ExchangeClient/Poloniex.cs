using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace Poloniex_REST_ExchangeClient
{
    class Poloniex
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        private long tradeID = 0L;
        public async Task createOrderBookJson(String orderBookData, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String[] topicSplit = topic.Split('-');
                String currency = null;
                if (topicSplit[1].Equals("BTC_ETH"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("BTC_LTC"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("BTC_DASH"))
                {
                    currency = "DSHBTC";
                }
                else if (topicSplit[1].Equals("BTC_XMR"))
                {
                    currency = "XMRBTC";
                }
                else if (topicSplit[1].Equals("BTC_XRP"))
                {
                    currency = "XRPBTC";
                }
                IDictionary<string, JToken> order = JsonConvert.DeserializeObject<JObject>(orderBookData);
                /*foreach (var dict in allOrderBook)
                {
                    if ((dict.Key.Equals("BTC_ETH")))
                    {
                        String[] currencySplit = dict.Key.Split('_');
                        String currency = null;
                        if (currencySplit[1].Equals("DASH"))
                        {
                            currency = "DSHBTC";
                        }
                        else
                        {
                            currency = currencySplit[1] + currencySplit[0];
                        }
                        IDictionary<string, JToken> order = dict.Value.ToObject<JObject>();*/

                        if (order.ContainsKey("asks") && order.ContainsKey("bids"))
                        {
                            JArray snapBidArray = order["bids"].ToObject<JArray>();
                            JArray snapAskArray = order["asks"].ToObject<JArray>();
                            JArray bidArray = new JArray();
                            JArray askArray = new JArray();
                            foreach (var bid in snapBidArray)
                            {
                                if ((bid[0].ToObject<Double>() != 0.0) && (bid[1].ToObject<Double>() != 0.0))
                                {
                                    JObject bidObj = JObject.FromObject(new
                                    {
                                        ExchangeName = "POLONIEX",
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
                                        ExchangeName = "POLONIEX",
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
                            try
                            {
                               await producer.ProduceAsync(topic, null, snapshotjson.ToString(Formatting.None));


                                log.Info(snapshotjson.ToString(Formatting.None));

                            }
                            catch (Exception ex)
                            {
                                log.Info(ex.Data + "\n");
                                log.Info(ex.Message);
                            }
                        }
                    //}

                //}
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
        public async Task createTickerJson(string tickerString)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var allTicker = JsonConvert.DeserializeObject<JObject>(tickerString);
                foreach (var dict in allTicker)
                {
                    if ((dict.Key.Equals("BTC_ETH")))
                    {
                        String[] currencySplit = dict.Key.Split('_');
                        String currency = null;
                        if (currencySplit[1].Equals("DASH"))
                        {
                            currency = "DSHBTC";
                        }
                        else
                        {
                            currency = currencySplit[1] + currencySplit[0];
                        }
                        IDictionary<string, JToken> ticker = dict.Value.ToObject<JObject>();
                        JObject tickerBook = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            TickerBook = JObject.FromObject(new
                            {
                                ExchangeName = "POLONIEX",
                                CurrencyPair = currency,
                                MachineTime = machineTime,
                                BidPrice = Convert.ToDouble(ticker["highestBid"]),
                                AskPrice = Convert.ToDouble(ticker["lowestAsk"]),
                                LastPrice = Convert.ToDouble(ticker["last"]),
                            })
                        });
                        try
                        {
                            await producer.ProduceAsync("Poloniex-" + dict.Key + "-Ticker", null, tickerBook.ToString(Formatting.None));
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
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }


        }
        public async Task createTradeBookJson(string tradeString, String topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                var tradeArray = JsonConvert.DeserializeObject<JArray>(tradeString);
                String[] topicSplit = topic.Split('-');

                String currency = null;
                if (topicSplit[1].Equals("BTC_ETH"))
                {
                    currency = "ETHBTC";
                }
                else if (topicSplit[1].Equals("BTC_LTC"))
                {
                    currency = "LTCBTC";
                }
                else if (topicSplit[1].Equals("BTC_DASH"))
                {
                    currency = "DSHBTC";
                }
                else if (topicSplit[1].Equals("BTC_XMR"))
                {
                    currency = "XMRBTC";
                }
                else if (topicSplit[1].Equals("BTC_XRP"))
                {
                    currency = "XRPBTC";
                }
                if (tradeArray[0]["tradeID"].ToObject<long>() > tradeID)
                {
                    foreach (JObject trade in tradeArray)
                    {
                        if (trade["tradeID"].ToObject<long>() > tradeID)
                        {
                            JObject tradeBook = JObject.FromObject(new
                            {
                                TimeStamp = timeStamp,
                                TradeBook = JObject.FromObject(new
                                {
                                    ExchangeName = "POLONIEX",
                                    CurrencyPair = currency,
                                    TradeId = trade["tradeID"].ToString(),
                                    TradeTime = DateTime.ParseExact(trade["date"].ToString(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture).AddSeconds(19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                    TradeSide = trade["type"].ToString(),
                                    Price = trade["rate"].ToObject<Double>(),
                                    Volume = trade["amount"].ToObject<Double>(),
                                    TotalBase = trade["total"].ToObject<Double>()
                                })
                            });
                            log.Info(tradeBook.ToString(Formatting.None));
                            try
                            {
                                await producer.ProduceAsync(topic, null, tradeBook.ToString(Formatting.None));
                            }
                            catch (Exception ex)
                            {
                                log.Info(ex.Data + "\n");
                                log.Info(ex.Message);
                            }
                        }
                    }
                    tradeID = tradeArray[0]["tradeID"].ToObject<long>();
                }
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
    }
}
