package co.biz.poloniex.sink;

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

import co.biz.poloniex.util.PoloniexSingletonClass;



/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class PoloniexXmrbtcADLSink<String> extends RichSinkFunction<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(PoloniexXmrbtcADLSink.class);
	
	@Override
	public void invoke(String XMRBTC) throws Exception {
		if(XMRBTC.toString().startsWith("{") && XMRBTC.toString().endsWith("}"))
		{
			JSONObject XMRBTCJson = new JSONObject((java.lang.String) XMRBTC);
			PoloniexSingletonClass obj = PoloniexSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (XMRBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(XMRBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/XMRBTC/RAWORDERBOOK/POLONIEX_XMRBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("MidPoint") && XMRBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/POLONIEX/XMRBTC/MIDPOINT/POLONIEX_XMRBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/POLONIEX/XMRBTC/TICKER/POLONIEX_XMRBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/XMRBTC/TRADEBOOK/POLONIEX_XMRBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("BestBid") && XMRBTCJson.has("BestAsk") && XMRBTCJson.getJSONObject("BestBid").length() > 0 && XMRBTCJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/XMRBTC/BESTBIDASKORDERBOOK/POLONIEX_XMRBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "POLONIEX_XMRBTC data is not written to ADL");
			}

		}

	}
}
