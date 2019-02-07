package co.biz.hitbtc.sink;

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

import co.biz.hitbtc.util.HitBtcSingletonClass;



public class HitBtcXmrbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(HitBtcXmrbtcADLSink.class);


	@Override
	public void invoke(String xmrbtc) throws Exception {

		if(xmrbtc.toString().startsWith("{") && xmrbtc.toString().endsWith("}"))
		{
			JSONObject xmrbtcJson = new JSONObject((java.lang.String) xmrbtc);
			HitBtcSingletonClass obj = HitBtcSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (xmrbtc + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(xmrbtcJson.has("Snapshot")  || xmrbtcJson.has("Updates"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/XMRBTC/RAWORDERBOOK/HITBTC_XMRBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(xmrbtcJson.has("MidPoint")&& xmrbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/XMRBTC/MIDPOINT/HITBTC_XMRBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(xmrbtcJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/XMRBTC/TICKER/HITBTC_XMRBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(xmrbtcJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/XMRBTC/TRADEBOOK/HITBTC_XMRBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(xmrbtcJson.has("BestBid") && xmrbtcJson.has("BestAsk")&& xmrbtcJson.getJSONObject("BestBid").length() > 0 && xmrbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/XMRBTC/BESTBIDASKORDERBOOK/HITBTC_XMRBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}

			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "HITBTC_xmrbtc data is not written to ADL");
			}

		}
		
	}
}
