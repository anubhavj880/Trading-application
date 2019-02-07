using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Exmo_ExchangeConnection_Client
{
    class Exmo
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        private long ethbtcTradeId = 0L;
        private long dshbtcTradeId = 0L;
        private long ltcbtcTradeId = 0L;
        public async Task createOrderBookJson(string orderBookData, string topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String[] topicSplit = topic.Split('-');
                JObject allOrderBook = JsonConvert.DeserializeObject<JObject>(orderBookData);

                String[] currencySplit = topicSplit[1].Split('_');
                String currency = null;
                if (currencySplit[0].Equals("DASH"))
                {
                    currency = "DSHBTC";
                }
                else
                {
                    currency = currencySplit[0] + currencySplit[1];
                }
                IDictionary<string, JToken> order = allOrderBook[topicSplit[1]].ToObject<JObject>();

                if (order.ContainsKey("ask") && order.ContainsKey("bid"))
                {

                    JArray snapBidArray = order["bid"].ToObject<JArray>();
                    JArray snapAskArray = order["ask"].ToObject<JArray>();
                    JArray bidArray = new JArray();
                    JArray askArray = new JArray();
                    foreach (var bid in snapBidArray)
                    {
                        if ((bid[0].ToObject<Double>() != 0.0) && (bid[1].ToObject<Double>() != 0.0))
                        {
                            JObject bidObj = JObject.FromObject(new
                            {
                                ExchangeName = "EXMO",
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
                                ExchangeName = "EXMO",
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
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
     }


        public async Task createTickerJson(string tickerData, string topic)
        {
            //log.Info("Entering to createTickerJson Exmo method: " + Thread.CurrentThread.ManagedThreadId + " - " + currencyPair);
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var allTicker = JsonConvert.DeserializeObject<JObject>(tickerData);
                foreach (var ticker in allTicker)
                {

                    if ((ticker.Key.Equals("ETH_BTC")) || (ticker.Key.Equals("LTC_BTC")) || (ticker.Key.Equals("DASH_BTC")))
                    {
                        var tickerjson = ticker.Value.ToObject<JObject>();
                        String[] currencySplit = ticker.Key.ToString().Split('_');
                        String currency = null;
                        if (currencySplit[0].Equals("DASH"))
                        {
                            currency = "DSHBTC";
                        }
                        else
                        {
                            currency = currencySplit[0] + currencySplit[1];
                        }
                        JObject tickerObj = JObject.FromObject(new
                        {
                            ExchangeName = "EXMO",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            BidPrice = tickerjson["buy_price"].ToObject<Double>(),
                            AskPrice = tickerjson["sell_price"].ToObject<Double>(),
                            LastPrice = tickerjson["last_trade"].ToObject<Double>()
                        });
                        JObject tickerBook = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            TickerBook = tickerObj,

                        });


                        await producer.ProduceAsync("Exmo-" + ticker.Key + "-Ticker", null, tickerBook.ToString(Formatting.None));
                        log.Info(tickerBook.ToString(Formatting.None));
                    }
                }
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }


        }
        public async Task createTradeBookJson(string tradeBookData, string topic)
        {
            try
            {
                var jsonData = JsonConvert.DeserializeObject<JObject>(tradeBookData);
                String[] topicSplit = topic.Split('-');
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var ethbtcPair = jsonData[topicSplit[1]].ToObject<JArray>();
                var dshbtcPair = jsonData[topicSplit[2]].ToObject<JArray>();
                var ltcbtcPair = jsonData[topicSplit[3]].ToObject<JArray>();
                if (ethbtcTradeId == 0 || ethbtcPair[0]["trade_id"].ToObject<long>() > ethbtcTradeId)
                {

                    foreach (var ethtradeJson in ethbtcPair)
                    {

                        JObject ethtrade = ethtradeJson.ToObject<JObject>();
                        if (ethbtcTradeId < ethtrade["trade_id"].ToObject<long>())
                        {
                            JObject tradeObj = JObject.FromObject(new
                            {
                                ExchangeName = "EXMO",
                                CurrencyPair = "ETHBTC",
                                MachineTime = machineTime,
                                TradeTime = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(ethtrade["date"].ToObject<Double>() + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                TradeId = ethtrade["trade_id"].ToString(),
                                TradeSide = ethtrade["type"].ToString(),
                                Price = ethtrade["price"].ToObject<Double>(),
                                Volume = ethtrade["quantity"].ToObject<Double>(),
                                TotalBase = (Convert.ToDouble(ethtrade["price"]) * Convert.ToDouble(ethtrade["quantity"]))
                            });
                            JObject tradeBook = JObject.FromObject(new
                            {
                                TimeStamp = timeStamp,
                                TradeBook = tradeObj,

                            });

                            try
                            {
                                await producer.ProduceAsync("Exmo-ETH_BTC-Trade", null, tradeBook.ToString(Formatting.None));
                            }
                            catch (Exception ex)
                            {
                                log.Info(ex.Data + "\n");
                                log.Info(ex.Message);
                            }
                            log.Info(tradeBook.ToString(Formatting.None));
                        }
                    }
                    ethbtcTradeId = ethbtcPair[0]["trade_id"].ToObject<long>();
                }

                if (dshbtcTradeId == 0 || dshbtcPair[0]["trade_id"].ToObject<long>() > dshbtcTradeId)
                {

                    foreach (var dshtradeJson in dshbtcPair)
                    {
                        JObject dshtrade = dshtradeJson.ToObject<JObject>();
                        if (dshbtcTradeId < (long)dshtradeJson.ToObject<JObject>()["trade_id"])
                        {
                            JObject tradeObj = JObject.FromObject(new
                            {
                                ExchangeName = "EXMO",
                                CurrencyPair = "DSHBTC",
                                MachineTime = machineTime,
                                TradeTime = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(dshtrade["date"].ToObject<Double>() + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                TradeId = dshtrade["trade_id"].ToString(),
                                TradeSide = dshtrade["type"].ToString(),
                                Price = dshtrade["price"].ToObject<Double>(),
                                Volume = dshtrade["quantity"].ToObject<Double>(),
                                TotalBase = (Convert.ToDouble(dshtrade["price"]) * Convert.ToDouble(dshtrade["quantity"]))
                            });
                            JObject tradeBook = JObject.FromObject(new
                            {
                                TimeStamp = timeStamp,
                                TradeBook = tradeObj,

                            });

                            try
                            {
                                await producer.ProduceAsync("Exmo-DASH_BTC-Trade", null, tradeBook.ToString(Formatting.None));
                            }
                            catch (Exception ex)
                            {
                                log.Info(ex.Data + "\n");
                                log.Info(ex.Message);
                            }
                            log.Info(tradeBook.ToString(Formatting.None));
                        }
                    }
                    dshbtcTradeId = dshbtcPair[0]["trade_id"].ToObject<long>();
                }

                if (ltcbtcTradeId == 0 || ltcbtcPair[0]["trade_id"].ToObject<long>() > ltcbtcTradeId)
                {

                    foreach (var ltctradeJson in ltcbtcPair)
                    {
                        JObject ltctrade = ltctradeJson.ToObject<JObject>();
                        if (ltcbtcTradeId < (long)ltctradeJson.ToObject<JObject>()["trade_id"])
                        {
                            JObject tradeObj = JObject.FromObject(new
                            {
                                ExchangeName = "EXMO",
                                CurrencyPair = "LTCBTC",
                                MachineTime = machineTime,
                                TradeTime = new DateTime(1970, 1, 1, 0, 0, 0, 0).AddSeconds(ltctrade["date"].ToObject<Double>() + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                TradeId = ltctrade["trade_id"].ToString(),
                                TradeSide = ltctrade["type"].ToString(),
                                Price = ltctrade["price"].ToObject<Double>(),
                                Volume = ltctrade["quantity"].ToObject<Double>(),
                                TotalBase = (Convert.ToDouble(ltctrade["price"]) * Convert.ToDouble(ltctrade["quantity"]))
                            });
                            JObject tradeBook = JObject.FromObject(new
                            {
                                TimeStamp = timeStamp,
                                TradeBook = tradeObj,

                            });

                            try
                            {
                                await producer.ProduceAsync("Exmo-LTC_BTC-Trade", null, tradeBook.ToString(Formatting.None));
                            }
                            catch (Exception ex)
                            {
                                log.Info(ex.Data + "\n");
                                log.Info(ex.Message);
                            }
                            log.Info(tradeBook.ToString(Formatting.None));
                        }
                    }
                    ltcbtcTradeId = ltcbtcPair[0]["trade_id"].ToObject<long>();
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
