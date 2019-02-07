using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Livecoin_ExchangeConnection_Client
{
    class Livecoin
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
                var allOrderBook = JsonConvert.DeserializeObject<JObject>(orderBookData);
                String[] topicSplit = topic.Split('-');

                foreach (var dict in allOrderBook)
                {
                    if ((dict.Key.Equals("ETH/BTC")) || (dict.Key.Equals("LTC/BTC")) || (dict.Key.Equals("DASH/BTC")) || (dict.Key.Equals("XMR/BTC")))
                    {
                        String[] currencySplit = dict.Key.Split('/');
                        String currency = null;
                        if (currencySplit[0].Equals("DASH"))
                        {
                            currency = "DSHBTC";
                        }
                        else
                        {
                            currency = currencySplit[0] + currencySplit[1];
                        }
                        var order = dict.Value.ToObject<JObject>();
                        IDictionary<string, JToken> dictionary = order;
                        if (dictionary.ContainsKey("asks") || dictionary.ContainsKey("bids"))
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
                                        ExchangeName = "LIVECOIN",
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
                                        ExchangeName = "LIVECOIN",
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
                                await producer.ProduceAsync("LiveCoin-" + currency + "-Order", null, snapshotjson.ToString(Formatting.None));
                                log.Info(snapshotjson.ToString(Formatting.None));
                            }
                            catch (Exception ex)
                            {
                                log.Info(ex.Data + "\n");
                                log.Info(ex.Message);
                            }

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

        public async Task createTickerJson(string tickerData, string topic)
        {
            try
            {
                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                String machineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30");
                var allTicker = JsonConvert.DeserializeObject<JArray>(tickerData);
                foreach (var ticker in allTicker)
                {
                    JObject tickerJobject = ticker.ToObject<JObject>();
                    var currencySymbol = tickerJobject["symbol"].ToString();
                    if ((currencySymbol.Equals("ETH/BTC")) || (currencySymbol.Equals("LTC/BTC")) || (currencySymbol.Equals("DASH/BTC")) || (currencySymbol.Equals("XMR/BTC")))
                    {

                        String[] currencySplit = tickerJobject["symbol"].ToString().Split('/');
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
                            ExchangeName = "LIVECOIN",
                            CurrencyPair = currency,
                            MachineTime = machineTime,
                            BidPrice = tickerJobject["best_bid"].ToObject<Double>(),
                            AskPrice = tickerJobject["best_ask"].ToObject<Double>(),
                            LastPrice = tickerJobject["last"].ToObject<Double>()
                        });
                        JObject tickerBook = JObject.FromObject(new
                        {
                            TimeStamp = timeStamp,
                            TickerBook = tickerObj,

                        });

                        try
                        {
                            await producer.ProduceAsync("LiveCoin-" + currency + "-Ticker", null, tickerBook.ToString(Formatting.None));
                        }
                        catch (Exception ex)
                        {
                            log.Info(ex.Data + "\n");
                            log.Info(ex.Message);
                        }
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

        public async Task createTradeBookJson(String tradeBookData, String topic)
        {
            try
            {

                String timeStamp = DateTime.Now.ToString("yyyy-MM-dd'T'HH:mm:ss.fff");
                var tradeJson = JsonConvert.DeserializeObject<JObject>(tradeBookData);
                String[] topicSplit = topic.Split('-');
                String tradeSide = "";
                if (tradeJson["type"].ToString().Equals("BUY"))
                {
                    tradeSide = "buy";
                }
                else
                {
                    tradeSide = "sell";
                }
                JObject tradeObj = JObject.FromObject(new
                {
                    ExchangeName = "LIVECOIN",
                    CurrencyPair = topicSplit[1].ToString(),
                    MachineTime = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeTime = new DateTime(1970, 1, 1).AddSeconds(Convert.ToDouble(tradeJson["time"]) + 19800).ToString("yyyy-MM-ddTHH:mm:ss.ffffff+05:30"),
                    TradeSide = tradeSide,
                    TradeId = tradeJson["id"].ToString(),
                    Price = tradeJson["price"].ToObject<Double>(),
                    Volume = tradeJson["quantity"].ToObject<Double>(),
                    TotalBase = (Convert.ToDouble(tradeJson["price"]) * Convert.ToDouble(tradeJson["quantity"]))
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
