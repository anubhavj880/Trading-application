using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Bittrex_ExchangeClient
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


        private static Bittrex bittrexObj = new Bittrex();
        private String uri = "";
        private String topicName = "";
        public async Task getMarketData(String uri, String topicName)
        {
            this.uri = uri;
            this.topicName = topicName;
            await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnMarketEvent);
                aTimer.Interval = 1000;
                aTimer.Enabled = true;
            });
        }


        private async void OnMarketEvent(object sender, ElapsedEventArgs ex)
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
                        response = null;
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
        /*  public async Task getMarketData(String uri, String topicName)
          {

              this.uri = uri;
              this.topicName = topicName;
              while (true)
              {
                  IRestResponse response = null;
                  try
                  {
                      var client = new RestClient("https://bittrex.com/api/v1.1/");
                      var request = new RestRequest(uri, Method.GET);
                      response = client.Execute(request);
                  }
                  catch (HttpRequestException e)
                  {
                      log.Info("Exception occured while: " + topicName);
                      log.Error("The Source of the exception: " + e.Source);
                      log.Error("The StackTrace of the exception: " + e.StackTrace);
                  }
                  catch (Exception e)
                  {
                      log.Info("Exception occured while: " + topicName);
                      log.Error("The Source of the exception: " + e.Source);
                      log.Error("The StackTrace of the exception: " + e.StackTrace);
                  }

                  if ((response != null))
                  {

                      await SendToKafka(response.Content, topicName);

                  }
                  else
                  {
                      log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                  }

              }
          }*/
        private async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');

            if (topicSplit[2].ToLower().Equals("order"))
            {
                await bittrexObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await bittrexObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await bittrexObj.createTickerJson(stringData, topic);
            }
        }
    }
}
