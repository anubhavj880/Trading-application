using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cexio_ExchangeConnetion_Client
{
    class Cexio
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Dictionary<string, object> config = new Dictionary<string, object> { { "bootstrap.servers", "192.168.1.158:9092" }, { "metadata.request.timeout.ms", 60000 }, { "request.timeout.ms", 5000 }, { "message.send.max.retries", 2 }, { "session.timeout.ms", 30000 }, { "socket.timeout.ms", 60000 } };
        private static Producer<Null, string> producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8));
        public async Task createOrderBookJson(string orderBookData, string topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var jsondata = JsonConvert.DeserializeObject<JObject>(orderBookData);
                JArray snapBidArray = jsondata["bids"].ToObject<JArray>();
                JArray snapAskArray = jsondata["asks"].ToObject<JArray>();
                JArray bidArray = new JArray();
                JArray askArray = new JArray();
                String[] topicSplit = topic.Split('-');

                foreach (var bid in snapBidArray)
                {
                    if ((bid[0].ToObject<Double>() != 0.0) && (bid[1].ToObject<Double>() != 0.0))
                    {
                        JObject bidObj = JObject.FromObject(new
                        {
                            ExchangeName = "CEXIO",
                            CurrencyPair = topicSplit[1],
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
                            ExchangeName = "CEXIO",
                            CurrencyPair = topicSplit[1],
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
                snapshotjson = null;
                jsondata = null;
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }
        }

        public async Task createTickerJson(string tickeString, string topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var jsondata = JsonConvert.DeserializeObject<JObject>(tickeString);
                String[] topicSplit = topic.Split('-');
                JObject tickerObj = JObject.FromObject(new
                {
                    ExchangeName = "CEXIO",
                    CurrencyPair = topicSplit[1],
                    MachineTime = machineTime,
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
                tickeString = null;
                jsondata = null;
            }
            catch (Exception ex)
            {
                log.Info(ex.Data + "\n");
                log.Info(ex.Message);
            }

        }

        public async Task createTradeBookJson(string tradeString, string topic)
        {

            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var jsondata = JsonConvert.DeserializeObject<JArray>(tradeString);
                var newJarray = jsondata.Last();
                newJarray.Remove();
                String[] topicSplit = topic.Split('-');
                foreach (var list in jsondata)
                {
                    JObject tradeObj = JObject.FromObject(new
                    {
                        ExchangeName = "CEXIO",
                        CurrencyPair = topicSplit[1],
                        MachineTime = machineTime,
                        TradeTime = new DateTime(1970, 1, 1).AddSeconds(Convert.ToDouble(list["date"]) + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                        TradeId = list["tid"].ToString(),
                        TradeSide = list["type"].ToString(),
                        Price = list["price"].ToObject<Double>(),
                        Volume = list["amount"].ToObject<Double>(),
                        TotalBase = (Convert.ToDouble(list["price"]) * Convert.ToDouble(list["amount"]))
                    });
                    JObject tradeBook = JObject.FromObject(new
                    {
                        TimeStamp = timeStamp,
                        TradeBook = tradeObj,

                    });

                    await producer.ProduceAsync(topic, null, tradeBook.ToString(Formatting.None));
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

