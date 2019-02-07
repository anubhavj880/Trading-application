using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Exmo_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static Exmo exmoObj = new Exmo();


        public async Task SendHttpRequest(String uri, String topicName)
        {

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
                            log.Info("Exception occured while: " + topicName);
                            log.Error("The Source of the exception: " + e.Source);
                            log.Error("The StackTrace of the exception: " + e.StackTrace);
                        }


                        if ((response != null) && response.IsSuccessStatusCode)
                        {
                            String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());


                            await SendToKafka(exchangeData, topicName);

                            exchangeData = null;
                        }
                        else
                        {
                            log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                        }
                    }
                    catch (Exception e)
                    {
                        log.Info("Exception occured while making the request : " + topicName);
                        log.Error("The Source of the exception: " + e.Source);
                        log.Error("The Message of the exception: " + e.Message);
                    }
                }
            }
        }


        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');
            if (topicSplit.Last().ToLower().Equals("order"))
            {
                await exmoObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit.Last().ToLower().Equals("trade"))
            {
                await exmoObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit.Last().ToLower().Equals("ticker"))
            {
                await exmoObj.createTickerJson(stringData, topic);
            }

        }
    }
}

