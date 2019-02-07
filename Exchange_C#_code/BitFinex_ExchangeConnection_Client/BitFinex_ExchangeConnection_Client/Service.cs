using System.ServiceProcess;
using System.Timers;
namespace BitFinex_ExchangeConnection_Client
{
    public class Service : ServiceBase
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        Timer tmrExecutor = new Timer();
        public Service()
        {
            ServiceName = BitfinexMain.ServiceName;
        }
        protected override void OnStart(string[] args)
        {
            BitfinexMain.Start(args);
            /*tmrExecutor.Elapsed += new ElapsedEventHandler(tmrExecutor_Elapsed);
            tmrExecutor.Interval = 1800000;
            tmrExecutor.Enabled = true;
            tmrExecutor.Start();*/

        }
        private void tmrExecutor_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            string[] args = null;
            log.Info("Restarted the Bitfinex Exchange Application.........................................................");
            BitfinexMain.Start(args);
        }
        protected override void OnStop()
        {
            tmrExecutor.Enabled = false;
            BitfinexMain.Stop();
        }
        /*protected override void OnStart(string[] args)
        {
            Program.Start(args);
        }

        protected override void OnStop()
        {
            Program.Stop();
        }*/
    }
}
