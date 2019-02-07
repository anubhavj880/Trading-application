using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Cexio_ExchangeConnetion_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Cexio cexioObj = new Cexio();
        private long cexioTradeId = 0;
        private String uri = "";
        private String topicName = "";
        public async Task SendHttpRequest(String uri, String topicName)
        {

            this.uri = uri;
            this.topicName = topicName;
            await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 3000;
                aTimer.Enabled = true;
            });

        }

        private async void OnTimedEvent(object sender, ElapsedEventArgs ex)
        {

            using (var client = new HttpClient())
            {
                try
                {
                    ServicePointManager.Expect100Continue = true;
                    ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    client.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17");
                    HttpResponseMessage response = null;
                    try
                    {
                        if (topicName.Contains("Trade"))
                        {

                            response = await client.GetAsync(uri + "?since=" + cexioTradeId);
                        }
                        else
                        {
                            response = await client.GetAsync(uri);
                        }
                    }
                    catch (HttpRequestException e)
                    {
                        log.Info("Exception occured while: " + topicName);
                        log.Error("The Source of the exception: " + e.Source);
                        log.Error("The StackTrace of the exception: " + e.StackTrace);
                    }
                    if ((response != null) && response.IsSuccessStatusCode)
                    {
                        String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());

                        if (topicName.Contains("Trade"))
                        {

                            var jsondata = JsonConvert.DeserializeObject<JArray>(exchangeData);
                            if (cexioTradeId == 0)
                            {

                                cexioTradeId = jsondata[0]["tid"].ToObject<long>();
                                await SendToKafka(exchangeData, topicName);
                            }
                            else if (cexioTradeId < jsondata[0]["tid"].ToObject<long>())
                            {
                                cexioTradeId = jsondata[0]["tid"].ToObject<long>();
                                await SendToKafka(exchangeData, topicName);
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
                    log.Info("Exception occured while making the request : " + topicName);
                    log.Info("The Source of the exception: " + e.Source);
                    log.Info("The Message of the exception: " + e.Message);
                }
            }

        }

        private static async Task SendToKafka(String stringData, String topic)
        {

            String[] topicSplit = topic.Split('-');

            if (topicSplit[2].ToLower().Equals("order"))
            {
                await cexioObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await cexioObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await cexioObj.createTickerJson(stringData, topic);
            }

        }
    }
}
