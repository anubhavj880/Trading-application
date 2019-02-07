package com.biz.bitfinex2datalake.sink;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.flink.hadoop.shaded.org.jboss.netty.util.Timeout;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.biz.bitfinex2datalake.util.BitfinexSingletonClass;
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
public class BitfinexEthBtcADLSink<String> extends RichSinkFunction<String> {
	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitfinexEthBtcADLSink.class);

	/*private static ADLStoreClient client = null;
	static 
	{
		try {
			 client = BitfinexSingletonClass.getInstance().getADLStoreClient();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}*/
 
	@Override
	public void invoke(String ethbtc) throws Exception {
		if(ethbtc.toString().startsWith("{") && ethbtc.toString().endsWith("}"))
		{
			JSONObject ethbtcJson = new JSONObject((java.lang.String) ethbtc);
			if((ethbtcJson.has("Snapshot") && ethbtcJson.getJSONArray("Snapshot").length() > 0) || (ethbtcJson.has("Updates") && ethbtcJson.getJSONArray("Updates").length() > 0))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITFINEX/ETHBTC/RAWORDERBOOK/BITFINEX_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("MidPoint") && ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITFINEX/ETHBTC/MIDPOINT/BITFINEX_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("TickerBook")&& ethbtcJson.getJSONObject("TickerBook").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITFINEX/ETHBTC/TICKER/BITFINEX_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if(ethbtcJson.has("TradeBook") && ethbtcJson.getJSONObject("TradeBook").length() > 0)
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITFINEX/ETHBTC/TRADEBOOK/BITFINEX_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

					char[] charbuf = (ethbtc.toString() + "\n").toCharArray(); 
					bufwriter.write(charbuf);

				} catch (IOException e) {

					slf4jLogger.error(e.getMessage());

				}
			}
			else if((ethbtcJson.has("BestBid") && ethbtcJson.getJSONObject("BestBid").length() > 0) && (ethbtcJson.has("BestAsk") && ethbtcJson.getJSONObject("BestAsk").length() > 0))
			{
				try (BufferedWriter bufwriter = new BufferedWriter(new FileWriter("/home/bizruntime/C-data/BITFINEX/ETHBTC/BESTBIDASKORDERBOOK/BITFINEX_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json",true))) {

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

			if((ethbtcJson.has("Snapshot") && ethbtcJson.getJSONArray("Snapshot").length() > 0) || (ethbtcJson.has("Updates") && ethbtcJson.getJSONArray("Updates").length() > 0))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/ETHBTC/RAWORDERBOOK/BITFINEX_ETHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
				else if(ethbtcJson.has("MidPoint") && ethbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/ETHBTC/MIDPOINT/BITFINEX_ETHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TickerBook")&& ethbtcJson.getJSONObject("TickerBook").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/ETHBTC/TICKER/BITFINEX_ETHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ethbtcJson.has("TradeBook") && ethbtcJson.getJSONObject("TradeBook").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/ETHBTC/TRADEBOOK/BITFINEX_ETHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if((ethbtcJson.has("BestBid") && ethbtcJson.getJSONObject("BestBid").length() > 0) && (ethbtcJson.has("BestAsk") && ethbtcJson.getJSONObject("BestAsk").length() > 0))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/ETHBTC/BESTBIDASKORDERBOOK/BITFINEX_ETHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else
			{
				slf4jLogger.info( "The data received:  " +  ethbtc);
			}

			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITFINEX_ETHBTC data is not written to ADL");
			}*/

		}
		
	}

	
	
	
}
