using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace Gatecoin_ExchangeConnection_client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Gatecoin gatecoinObj = new Gatecoin();
        private String uri = "";
        private String topicName = "";
        private long gatecoinTradeId = 0;
        public async Task SendHttpRequest(String uri, String topicName)
        {

            this.uri = uri;
            this.topicName = topicName;
            while (true)
            {
                using (var client = new HttpClient())
                {
                    try
                    {
                        client.DefaultRequestHeaders.Accept.Clear();
                        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                        client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17");
                        HttpResponseMessage response = null;
                        try
                        {
                            response = await client.GetAsync(uri);
                        }
                        catch (HttpRequestException e)
                        {
                            log.Info("Exception occured while making the request to the topic : " + topicName);
                            log.Info("The Source of the exception: " + e.Source);
                            log.Info("The Message of the exception: " + e.Message);
                        }
                        if ((response != null) && response.IsSuccessStatusCode)
                        {
                            String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());
                            if (topicName.Contains("Trade"))
                            {
                                IDictionary<string, JToken> jsonData = JsonConvert.DeserializeObject<JObject>(exchangeData);
                                String[] topicSplit = topicName.Split('-');
                                if(jsonData.ContainsKey("transactions") && jsonData["transactions"].ToObject<JArray>().Count > 0)
                                {
                                    var tradeArray = jsonData["transactions"].ToObject<JArray>();

                                    if (gatecoinTradeId == 0)
                                    {
                                        gatecoinTradeId = (long)tradeArray[0]["transactionId"];
                                        foreach (var tradeJson in tradeArray)
                                        {

                                            await SendToKafka(tradeJson.ToString(), topicName);
                                        }
                                    }
                                    else
                                    {
                                        foreach (var tradeJson in tradeArray)
                                        {
                                            if (gatecoinTradeId < (long)tradeJson.ToObject<JObject>()["transactionId"])
                                            {
                                                await SendToKafka(tradeJson.ToString(), topicName);
                                            }
                                        }
                                        gatecoinTradeId = tradeArray[0]["transactionId"].ToObject<long>();
                                    }
                                }
                        
                            }

                            else
                            {
                                await SendToKafka(exchangeData, topicName);
                            }
                        }

                        else
                        {
                            log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Info("Exception occured while: " + topicName);
                        log.Info("The Source of the exception: " + e.Source);
                        log.Info("The Message of the exception: " + e.Message);
                    }
                }

            }
        }
        private static async Task SendToKafka(String stringData, String topic)
        {

            String[] topicSplit = topic.Split('-');

            if (topicSplit[2].ToLower().Equals("order"))
            {
                await gatecoinObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await gatecoinObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await gatecoinObj.createTickerJson(stringData, topic);
            }
        }
    }
}
