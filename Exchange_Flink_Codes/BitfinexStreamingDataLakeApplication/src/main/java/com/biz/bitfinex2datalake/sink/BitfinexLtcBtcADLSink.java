package com.biz.bitfinex2datalake.sink;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
public class BitfinexLtcBtcADLSink<String> extends RichSinkFunction<String> {
	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitfinexLtcBtcADLSink.class);

	private static ADLStoreClient client = null;
	static 
	{
		try {
			 client = BitfinexSingletonClass.getInstance().getADLStoreClient();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	@Override
	public void invoke(String LTCBTC) throws Exception {

		if(LTCBTC.toString().startsWith("{") && LTCBTC.toString().endsWith("}"))
		{
			JSONObject LTCBTCJson = new JSONObject((java.lang.String) LTCBTC);
			
			byte[] myBuffer = (LTCBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();
			
			if((LTCBTCJson.has("Snapshot")) || (LTCBTCJson.has("Updates")))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/LTCBTC/RAWORDERBOOK/BITFINEX_LTCBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("MidPoint") && LTCBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/LTCBTC/MIDPOINT/BITFINEX_LTCBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/LTCBTC/TICKER/BITFINEX_LTCBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/LTCBTC/TRADEBOOK/BITFINEX_LTCBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if((LTCBTCJson.has("BestBid") && LTCBTCJson.getJSONObject("BestBid").length() > 0) && (LTCBTCJson.has("BestAsk") && LTCBTCJson.getJSONObject("BestAsk").length() > 0))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/LTCBTC/BESTBIDASKORDERBOOK/BITFINEX_LTCBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else
			{
				slf4jLogger.info( "The data received:  " +  LTCBTC);
			}
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITFINEX_LTCBTC_ORDER data is not written to ADL");
			}

		}
		
	}
}
