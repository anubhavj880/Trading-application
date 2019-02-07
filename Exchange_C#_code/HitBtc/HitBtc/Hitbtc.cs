using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace HitBtc
{
    class Hitbtc
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        public async Task createOrderBookJson(string orderBookData, string topic)
        {
            try
            {
                IDictionary<string, JToken> jsonData = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                String[] topicSplit = topic.Split('-');
                String currency = "";
                if (topicSplit[1].Equals("DASHBTC"))
                {
                    currency = "DSHBTC";
                }
                else
                {
                    currency = topicSplit[1];
                }
                if (jsonData.ContainsKey("MarketDataSnapshotFullRefresh"))
                {
                    var snapjson = jsonData["MarketDataSnapshotFullRefresh"].ToObject<JObject>();
                    var snapSeqNo = snapjson["snapshotSeqNo"].ToObject<long>();
                    JArray snapAskArray = snapjson["ask"].ToObject<JArray>();
                    JArray snapBidArray = snapjson["bid"].ToObject<JArray>();
                    JArray bidArray = new JArray();
                    JArray askArray = new JArray();
                    foreach (var askjson in snapAskArray)
                    {
                        JObject askobj = JObject.FromObject(new
                        {
                            ExchangeName = "HITBTC",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            OrderSide = "asks",
                            Price = askjson["price"].ToObject<Double>(),
                            Quantity = askjson["size"].ToObject<Double>()
                        });
                        askArray.Add(askobj);
                    }
                    foreach (var bidjson in snapBidArray)
                    {
                        JObject bidobj = JObject.FromObject(new
                        {
                            ExchangeName = "HITBTC",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            OrderSide = "bids",
                            Price = bidjson["price"].ToObject<Double>(),
                            Quantity = bidjson["size"].ToObject<Double>()
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

                else if (jsonData.ContainsKey("MarketDataIncrementalRefresh"))
                {
                    var updatejson = jsonData["MarketDataIncrementalRefresh"].ToObject<JObject>();
                    var updateSeqNo = updatejson["seqNo"].ToObject<long>();
                    JArray updateAskArray = updatejson["ask"].ToObject<JArray>();
                    JArray updateBidArray = updatejson["bid"].ToObject<JArray>();
                    JArray updatesArray = new JArray();

                    if (updateAskArray.Count != 0)
                    {
                        foreach (var askjson in updateAskArray)
                        {
                            JObject askobj = JObject.FromObject(new
                            {
                                ExchangeName = "HITBTC",
                                CurrencyPair = currency,
                                MachineTime = new DateTime(1970, 1, 1).AddMilliseconds(Convert.ToDouble(updatejson["timestamp"]) + 19800000).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                OrderSide = "asks",
                                Price = askjson["price"].ToObject<Double>(),
                                Quantity = askjson["size"].ToObject<Double>()
                            });
                            updatesArray.Add(askobj);
                        }
                    }
                    if (updateBidArray.Count != 0)
                    {
                        foreach (var bidjson in updateBidArray)
                        {
                            JObject bidobj = JObject.FromObject(new
                            {
                                ExchangeName = "HITBTC",
                                CurrencyPair = currency,
                                MachineTime = new DateTime(1970, 1, 1).AddMilliseconds(Convert.ToDouble(updatejson["timestamp"]) + 19800000).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                OrderSide = "bids",
                                Price = bidjson["price"].ToObject<Double>(),
                                Quantity = bidjson["size"].ToObject<Double>()
                            });
                            updatesArray.Add(bidobj);
                        }
                    }
                    if (updatesArray.Count != 0)
                    {
                        JObject updateObj = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            SequenceNumber = updateSeqNo,
                            Updates = updatesArray,
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
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }
        public async Task createTickerJson(string tickerdata, string topic)
        {
            try
            {
                var jsondata = JsonConvert.DeserializeObject<JObject>(tickerdata);
                String[] topicSplit = topic.Split('-');
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String currency = "";
                if (topicSplit[1].Equals("DASHBTC"))
                {
                    currency = "DSHBTC";
                }
                else
                {
                    currency = topicSplit[1];
                }
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "HITBTC",
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
        public async Task createTradeBookJson(string tradeBookdata, string topic)
        {
            try
            {
                var jsonData = JsonConvert.DeserializeObject<JObject>(tradeBookdata);
                var bidAskDict = jsonData["MarketDataIncrementalRefresh"].ToObject<JObject>();
                String[] topicSplit = topic.Split('-');
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String currency = "";
                if (topicSplit[1].Equals("DASHBTC"))
                {
                    currency = "DSHBTC";
                }
                else
                {
                    currency = topicSplit[1];
                }
                foreach (var dic in bidAskDict)
                {
                    if ((dic.Key.Equals("trade")))
                    {
                        foreach (var list in (dic.Value))
                        {
                            var time = list["timestamp"];
                            JObject tradeObj = JObject.FromObject(new
                            {
                                ExchangeName = "HITBTC",
                                CurrencyPair = currency,
                                MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                TradeTime = new DateTime(1970, 1, 1).AddMilliseconds(Convert.ToDouble(time) + 19800000).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                                TradeSide = list["side"].ToString(),
                                TradeId = list["tradeId"].ToString(),
                                Price = list["price"].ToObject<Double>(),
                                Volume = list["size"].ToObject<Double>(),
                                TotalBase = (Convert.ToDouble(list["price"]) * Convert.ToDouble(list["size"]))
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
