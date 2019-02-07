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
public class BitfinexXmrBtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitfinexXmrBtcADLSink.class);
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
	public void invoke(String XMRBTC) throws Exception {
		if(XMRBTC.toString().startsWith("{") && XMRBTC.toString().endsWith("}"))
		{
			JSONObject XMRBTCJson = new JSONObject((java.lang.String) XMRBTC);
			
			byte[] myBuffer = (XMRBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if((XMRBTCJson.has("Snapshot")) || (XMRBTCJson.has("Updates")))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/BITFINEX/XMRBTC/RAWORDERBOOK/BITFINEX_XMRBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("MidPoint") && XMRBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/XMRBTC/MIDPOINT/BITFINEX_XMRBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/XMRBTC/TICKER/BITFINEX_XMRBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITFINEX/XMRBTC/TRADEBOOK/BITFINEX_XMRBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if((XMRBTCJson.has("BestBid") && XMRBTCJson.getJSONObject("BestBid").length() > 0) && (XMRBTCJson.has("BestAsk") && XMRBTCJson.getJSONObject("BestAsk").length() > 0))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/BITFINEX/XMRBTC/BESTBIDASKORDERBOOK/BITFINEX_XMRBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else
			{
				slf4jLogger.info( "The data received:  " +  XMRBTC);
			}
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITFINEX_XMRBTC_ORDER data is not written to ADL");
			}

		}
		
	}
}
