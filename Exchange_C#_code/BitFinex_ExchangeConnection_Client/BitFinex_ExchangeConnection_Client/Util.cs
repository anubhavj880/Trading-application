using System;
using System.Threading.Tasks;
using System.Timers;
using WebSocketSharp;

namespace BitFinex_ExchangeConnection_Client
{
    class Util
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static Bitfinex bitfinexObj = new Bitfinex();
        private WebSocket ws = null;
        public  async Task webSocketConnect(String subscribeSchema, String uri, String topicName)
        {

            await Task.Run(() =>
            {
                ws = new WebSocket(uri);
                ws.EmitOnPing = true;
                System.Timers.Timer aTimer = new System.Timers.Timer();
                aTimer.Elapsed += new ElapsedEventHandler(OnTimedEvent);
                aTimer.Interval = 900000;
                aTimer.Enabled = true;
                ws.OnOpen += (sender, e) =>
                {
                    log.Info(ws.ReadyState);
                    log.Info("Websocket Connected for: " + topicName);

                };

                ws.OnMessage +=  async(sender, e) =>
                {
                    await SendToKafka(e.Data, topicName);
                };

                ws.OnError += (sender, e) =>
               {
                   log.Error("Error occured: " + topicName + "\n");
                   log.Info(e.Message + "\n");

               };
                ws.OnClose += (sender, e) =>
               {
                   log.Info("Websocket getting closed for the following topic : " + topicName + "\n");
                   try
                   {
                       while (!(ws.ReadyState.ToString().Equals("Open")))
                       {
                           log.Info("Trying to reconnect" + "\n");
                           log.Info(ws.ReadyState);
                           ws.Connect();
                           ws.Send(subscribeSchema);
                           Task.Delay(1000);

                       }
                   }
                   catch(Exception ex)
                   {
                       log.Error("The following exception occured inside OnClose: " + ex.Message);
                   }
               };
                ws.Connect();
                ws.Send(subscribeSchema);

            });
        }
        private void OnTimedEvent(object sender, ElapsedEventArgs e)
        {
            if (ws != null)
            {
                ws.Close();
            }
        }
        private static async Task SendToKafka(String stringData, String topic)
        {
            String[] topicSplit = topic.Split('-');
            if (topicSplit[2].ToLower().Equals("order"))
            {
                await bitfinexObj.createOrderBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("trade"))
            {
                await bitfinexObj.createTradeBookJson(stringData, topic);
            }
            else if (topicSplit[2].ToLower().Equals("ticker"))
            {
                await bitfinexObj.createTickerJson(stringData, topic);
            }
        }
    }
}
