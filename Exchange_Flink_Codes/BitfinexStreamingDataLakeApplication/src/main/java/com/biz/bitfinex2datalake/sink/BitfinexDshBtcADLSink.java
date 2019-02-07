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
public class BitfinexDshBtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitfinexDshBtcADLSink.class);

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
	public void invoke(String DSHBTC) throws Exception {
		if(DSHBTC.toString().startsWith("{") && DSHBTC.toString().endsWith("}"))
		{
			JSONObject DSHBTCJson = new JSONObject((java.lang.String) DSHBTC);
			
			byte[] myBuffer = (DSHBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if((DSHBTCJson.has("Snapshot")) || (DSHBTCJson.has("Updates")))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/DSHBTC/RAWORDERBOOK/BITFINEX_DSHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("MidPoint") && DSHBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/DSHBTC/MIDPOINT/BITFINEX_DSHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/BITFINEX/DSHBTC/TICKER/BITFINEX_DSHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/DSHBTC/TRADEBOOK/BITFINEX_DSHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if((DSHBTCJson.has("BestBid") && DSHBTCJson.getJSONObject("BestBid").length() > 0) && (DSHBTCJson.has("BestAsk") && DSHBTCJson.getJSONObject("BestAsk").length() > 0))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/DSHBTC/BESTBIDASKORDERBOOK/BITFINEX_DSHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else
			{
				slf4jLogger.info( "The data received:  " +  DSHBTC);
			}
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITFINEX_ORDER data is not written to ADL");
			}

		}
		
	}
}
