using PusherClient;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace TheRock_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static TheRock rockObj = new TheRock();
        private String uri = "";
        private String topicName = "";
        public static async Task Pusher(String appKey, String kafkaTopic, String subscribeTopic)
        {
            await Task.Run(() =>
             {
                 Pusher _pusher = new Pusher(appKey);
                 _pusher.Connect();
                 _pusher.ConnectionStateChanged += (sender, state) =>
                 {
                     log.Info("Connection state: " + state.ToString());
                 };
                 _pusher.Error += (sender, error) =>
                {

                    log.Info("Pusher Error has occured: " + " - " + error.ToString());
                };

                 _pusher.Connected += (sender) =>
                 {

                     Channel _myChannel = null;
                     String[] topicSplit = kafkaTopic.Split('-');
                     if (topicSplit[2].ToLower().Equals("trade"))
                     {
                         _myChannel = _pusher.Subscribe(subscribeTopic);
                         _myChannel.Subscribed += (subscribedSender) =>
                        {
                            log.Info("Subscribed to: " + kafkaTopic);


                        };
                         _myChannel.Bind("last_trade", async (data) =>
                         {
                             await SendToKafka(Convert.ToString(data), kafkaTopic);
                         });
                     }
                     else if (topicSplit[2].ToLower().Equals("order"))
                     {
                         _myChannel = _pusher.Subscribe(topicSplit[1]);
                         _myChannel.Subscribed += (subscribedSender) =>
                        {

                            log.Info("Subscribed to: " + kafkaTopic);
                        };
                         _myChannel.Bind("orderbook", async (data) =>
                        {

                            await SendToKafka(Convert.ToString(data), kafkaTopic);

                        });
                     }
                 };
             });
        }
        public async Task SendHttpRequest(String uri, String topicName)
        {
            this.uri = uri;
            this.topicName = topicName;
            await Task.Run(() =>
            {
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 1000;
                aTimer.Enabled = true;
            });

        }

        private async void OnTimedEvent(object sender, ElapsedEventArgs ex)
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
                        log.Info("The Source of the exception: " + e.Source);
                        log.Info("The Message of the exception: " + e.Message);
                    }


                    if ((response != null) && response.IsSuccessStatusCode)
                    {
                        String exchangeData = Encoding.UTF8.GetString(await response.Content.ReadAsByteArrayAsync());

                        await SendToKafka(exchangeData, topicName);
                    }
                    else
                    {
                        log.Error("Response for the request to the topic : " + topicName + " _________" + response);
                    }
                }
                catch (Exception e)
                {
                    log.Info("Exception occured while making request to : " + topicName);
                    log.Info("The Source of the exception: " + e.Source);
                    log.Info("The Message of the exception: " + e.Message);
                }
            }

        }

        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');
            if (topicSplit[2].ToLower().Equals("trade"))
            {
                await rockObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("order"))
            {
                await rockObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await rockObj.createTickerJson(stringData, topic);
            }
        }
    }
}
