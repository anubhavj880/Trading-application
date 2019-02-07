package co.biz.bittrex2datalake.sink;

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

import co.biz.bittrex2datalake.util.BittrexSingletonClass;





/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class BittrexEthbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BittrexEthbtcADLSink.class);
	/*private static ADLStoreClient client = null;
	static 
	{
		try {
			 client = BittrexSingletonClass.getInstance().getADLStoreClient();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
*/

	@Override
	public void invoke(String ethbtc) throws Exception {
		if(ethbtc.toString().startsWith("{") && ethbtc.toString().endsWith("}"))
		{
			JSONObject ethbtcJson = new JSONObject((java.lang.String) ethbtc);
			
			if(ethbtcJson.has("Snapshot") )
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITTREX/ETHBTC/RAWORDERBOOK/BITTREX_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("MidPoint")&& ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITTREX/ETHBTC/MIDPOINT/BITTREX_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("TickerBook")&& ethbtcJson.getJSONObject("TickerBook").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITTREX/ETHBTC/TICKER/BITTREX_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("TradeBook") && ethbtcJson.getJSONObject("TradeBook").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITTREX/ETHBTC/TRADEBOOK/BITTREX_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if((ethbtcJson.has("BestBid") && ethbtcJson.getJSONObject("BestBid").length() > 0) && (ethbtcJson.has("BestAsk") && ethbtcJson.getJSONObject("BestAsk").length() > 0))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITTREX/ETHBTC/BESTBIDASKORDERBOOK/BITTREX_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

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
			
			
			
			/*byte[] myBuffer = (ethbtc + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(ethbtcJson.has("Snapshot") )
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/BITTREX/ETHBTC/RAWORDERBOOK/BITTREX_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("MidPoint")&& ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/ETHBTC/MIDPOINT/BITTREX_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TickerBook")&& ethbtcJson.getJSONObject("TickerBook").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/ETHBTC/TICKER/BITTREX_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TradeBook")&& ethbtcJson.getJSONObject("TradeBook").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/ETHBTC/TRADEBOOK/BITTREX_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if((ethbtcJson.has("BestBid") && ethbtcJson.getJSONObject("BestBid").length() > 0) && (ethbtcJson.has("BestAsk") && ethbtcJson.getJSONObject("BestAsk").length() > 0))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/ETHBTC/BESTBIDASKORDERBOOK/BITTREX_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}	
			else
			{
				slf4jLogger.info( "The data received:  " +  ethbtc);
			}	
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITTREX_ETHBTC data is not written to ADL");
			}*/

		}

	}
}
