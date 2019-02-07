using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace BitFinex_ExchangeConnection_Client
{
    class Bitfinex
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        public async Task createOrderBookJson(String orderBookData, String topic)
        {
            try
            {
                var jsonData = JsonConvert.DeserializeObject(orderBookData);
                String[] topicSplit = topic.Split('-');
                if (jsonData is JArray)
                {

                    JArray jsonArray = JsonConvert.DeserializeObject<JArray>(orderBookData);
                    String OrderSideString = "";
                    if (jsonArray.Count == 2 && (jsonArray[1] is JArray))
                    {
                        String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                        String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                        long channelId = jsonArray[0].ToObject<long>();
                        JArray snapshotDataArray = jsonArray[1].ToObject<JArray>();
                        JArray snapshotJarray = new JArray();
                        JObject channel = JObject.FromObject(new
                        {
                            ChannelId = channelId
                        });
                        snapshotJarray.Add(channel);
                        foreach (var snapshotData in snapshotDataArray)
                        {
                            if (snapshotData[2].ToObject<Double>() > 0.0)
                            {
                                OrderSideString = "bids";
                            }
                            else
                            {
                                OrderSideString = "asks";
                            }
                            JObject obj = JObject.FromObject(new
                            {
                                ExchangeName = "BITFINEX",
                                CurrencyPair = topicSplit[1],
                                MachineTime = machineTime,
                                OrderSide = OrderSideString,
                                OrderId = snapshotData[0].ToObject<long>(),
                                Price = snapshotData[1].ToObject<Double>(),
                                Quantity = Math.Abs(snapshotData[2].ToObject<Double>())
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

                            log.Info(orderbookSnapShot.ToString(Formatting.None));
                        }
                        catch (Exception ex)
                        {
                            log.Info(ex.Data + "\n");
                            log.Info(ex.Message);
                        }


                    }
                    else if (jsonArray.Count == 4)
                    {

                        JArray updateList = new JArray();
                        long updatechannelId = jsonArray[0].ToObject<long>();
                        String updateTime = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                        String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                        JObject channel = JObject.FromObject(new
                        {
                            ChannelId = updatechannelId
                        });
                        updateList.Add(channel);
                        if (jsonArray[3].ToObject<Double>() > 0.0)
                        {
                            OrderSideString = "bids";
                        }
                        else
                        {
                            OrderSideString = "asks";
                        }
                        JObject obj = JObject.FromObject(new
                        {
                            ExchangeName = "BITFINEX",
                            CurrencyPair = topicSplit[1],
                            MachineTime = machineTime,
                            OrderSide = OrderSideString,
                            OrderId = jsonArray[1].ToObject<long>(),
                            Price = jsonArray[2].ToObject<Double>(),
                            Quantity = Math.Abs(jsonArray[3].ToObject<Double>())
                        });
                        updateList.Add(obj);
                        JObject orderbookUpdates = JObject.FromObject(new
                        {
                            TimeStamp = updateTime,
                            Updates = updateList
                        });
                        try
                        {
                            await producer.ProduceAsync(topic, null, orderbookUpdates.ToString(Formatting.None));

                            log.Info(orderbookUpdates.ToString(Formatting.None));
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
      
        public async Task createTickerJson(string tradeString, String topic)
        {
            try
            {
                var jsonData = JsonConvert.DeserializeObject(tradeString);
                String[] topicSplit = topic.Split('-');
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                if (jsonData is JArray && ((JArray)jsonData).Count == 11)
                {
                    JArray jsonArray = ((JArray)jsonData);
                    JObject tickerObj = JObject.FromObject(new
                    {
                        ExchangeName = "BITFINEX",
                        CurrencyPair = topicSplit[1],
                        MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        BidPrice = jsonArray[1].ToObject<Double>(),
                        BidSize = jsonArray[2].ToObject<Double>(),
                        AskPrice = jsonArray[3].ToObject<Double>(),
                        AskSize = jsonArray[4].ToObject<Double>(),
                        LastPrice = jsonArray[7].ToObject<Double>()
                    });
                    JObject tickerBook = JObject.FromObject(new
                    {
                        TimeStamp = timeStamp,
                        TickerBook = tickerObj,

                    });

                    try
                    {
                        await producer.ProduceAsync(topic, null, tickerBook.ToString(Formatting.None));
                    }
                    catch (Exception ex)
                    {
                        log.Info(ex.Data + "\n");
                        log.Info(ex.Message);
                    }
                    log.Info("Bitfinex ETHBTC ticker data:  " + tickerBook.ToString(Formatting.None));
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
                var jsonData = JsonConvert.DeserializeObject(tradeString);
                String[] topicSplit = topic.Split('-');
                if (jsonData is JArray && ((JArray)jsonData).Count == 7)
                {
                    JArray jsonArray = ((JArray)jsonData);
                    String tradeSideString = "";
                    String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                    if (jsonArray[6].ToObject<int>() > 0)
                    {
                        tradeSideString = "buy";
                    }
                    else
                    {
                        tradeSideString = "sell";
                    }
                    JObject tradeObj = JObject.FromObject(new
                    {
                        ExchangeName = "BITFINEX",
                        CurrencyPair = topicSplit[1],
                        TradeId = jsonArray[3].ToString(),
                        TradeTime = new DateTime(1970, 1, 1).AddSeconds(jsonArray[4].ToObject<long>() + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        TradeSide = tradeSideString,
                        Price = jsonArray[5].ToObject<Double>(),
                        Volume = Math.Abs(jsonArray[6].ToObject<Double>()),
                        TotalBase = (jsonArray[5].ToObject<Double>()) * (Math.Abs(jsonArray[6].ToObject<Double>()))

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
