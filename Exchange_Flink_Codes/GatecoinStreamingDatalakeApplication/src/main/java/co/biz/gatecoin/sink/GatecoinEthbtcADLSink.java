package co.biz.gatecoin.sink;

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

/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class GatecoinEthbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(GatecoinEthbtcADLSink.class);
	
	@Override
	public void invoke(String ethbtc) throws Exception {
		if(ethbtc.toString().startsWith("{") && ethbtc.toString().endsWith("}"))
		{
			JSONObject ethbtcJson = new JSONObject((java.lang.String) ethbtc);
			if(ethbtcJson.has("Snapshot"))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GATECOIN/ETHBTC/RAWORDERBOOK/GATECOIN_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}				}
			else if(ethbtcJson.has("MidPoint") && ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GATECOIN/ETHBTC/MIDPOINT/GATECOIN_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}				}
			else if(ethbtcJson.has("TickerBook"))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GATECOIN/ETHBTC/TICKER/GATECOIN_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}			}
			else if(ethbtcJson.has("TradeBook"))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GATECOIN/ETHBTC/TRADEBOOK/GATECOIN_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}			}
			else if(ethbtcJson.has("BestBid") && ethbtcJson.has("BestAsk") && ethbtcJson.getJSONObject("BestBid").length() > 0 && ethbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/GATECOIN/ETHBTC/BESTBIDASKORDERBOOK/GATECOIN_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}			}		
			/*GATECOINSingletonClass obj = GATECOINSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (ethbtc + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(ethbtcJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GATECOIN/ETHBTC/RAWORDERBOOK/GATECOIN_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("MidPoint") && ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GATECOIN/ETHBTC/MIDPOINT/GATECOIN_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GATECOIN/ETHBTC/TICKER/GATECOIN_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GATECOIN/ETHBTC/TRADEBOOK/GATECOIN_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("BestBid") && ethbtcJson.has("BestAsk") && ethbtcJson.getJSONObject("BestBid").length() > 0 && ethbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/GATECOIN/ETHBTC/BESTBIDASKORDERBOOK/GATECOIN_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "GATECOIN_ETHBTC data is not written to ADL");
			}*/

		}

	}
}

