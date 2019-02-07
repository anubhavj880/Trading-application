using System.ServiceProcess;

namespace Gemini_ExchangeClient
{
    public class Service : ServiceBase
    {
        public Service()
        {
            ServiceName = GeminiMain.ServiceName;
        }

        protected override void OnStart(string[] args)
        {
            GeminiMain.Start(args);
        }

        protected override void OnStop()
        {
            GeminiMain.Stop();
        }
    }
}
