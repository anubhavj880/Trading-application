package co.biz.gemini.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.Core;
import com.microsoft.azure.datalake.store.OperationResponse;
import com.microsoft.azure.datalake.store.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

import co.biz.gemini.util.GeminiSingletonClass;

/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class GeminiEthbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(GeminiEthbtcADLSink.class);


	@Override
	public void invoke(String ethbtc) throws Exception {

		if(ethbtc.toString().startsWith("{") && ethbtc.toString().endsWith("}"))
		{
			JSONObject ethbtcJson = new JSONObject((java.lang.String) ethbtc);
			if(ethbtcJson.has("Snapshot")  || ethbtcJson.has("Updates"))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GEMINI/ETHBTC/RAWORDERBOOK/GEMINI_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("MidPoint")&& ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GEMINI/ETHBTC/MIDPOINT/GEMINI_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("TickerBook"))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GEMINI/ETHBTC/TICKER/GEMINI_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("TradeBook"))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GEMINI/ETHBTC/TRADEBOOK/GEMINI_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("BestBid") && ethbtcJson.has("BestAsk")&& ethbtcJson.getJSONObject("BestBid").length() > 0 && ethbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GEMINI/ETHBTC/BESTBIDASKORDERBOOK/GEMINI_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else
			{
				slf4jLogger.info( "The data received:  " +  ethbtc);
			}
			/*GeminiSingletonClass obj = GeminiSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (ethbtc + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(ethbtcJson.has("Snapshot")  || ethbtcJson.has("Updates"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GEMINI/ETHBTC/RAWORDERBOOK/GEMINI_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("MidPoint")&& ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GEMINI/ETHBTC/MIDPOINT/GEMINI_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GEMINI/ETHBTC/TICKER/GEMINI_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GEMINI/ETHBTC/TRADEBOOK/GEMINI_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("BestBid") && ethbtcJson.has("BestAsk")&& ethbtcJson.getJSONObject("BestBid").length() > 0 && ethbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GEMINI/ETHBTC/BESTBIDASKORDERBOOK/GEMINI_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}

			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "GEMINI_ETHBTC data is not written to ADL");
			}*/

		}
		
	}
}
