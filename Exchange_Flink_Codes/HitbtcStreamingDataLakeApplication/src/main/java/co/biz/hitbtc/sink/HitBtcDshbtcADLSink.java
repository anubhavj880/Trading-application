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



public class HitBtcDshbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(HitBtcDshbtcADLSink.class);


	@Override
	public void invoke(String dshbtc) throws Exception {

		if(dshbtc.toString().startsWith("{") && dshbtc.toString().endsWith("}"))
		{
			JSONObject dshbtcJson = new JSONObject((java.lang.String) dshbtc);
			HitBtcSingletonClass obj = HitBtcSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (dshbtc + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(dshbtcJson.has("Snapshot")  || dshbtcJson.has("Updates"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/DSHBTC/RAWORDERBOOK/HITBTC_DSHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(dshbtcJson.has("MidPoint")&& dshbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/HITBTC/DSHBTC/MIDPOINT/HITBTC_DSHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(dshbtcJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/DSHBTC/TICKER/HITBTC_DSHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(dshbtcJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/DSHBTC/TRADEBOOK/HITBTC_DSHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(dshbtcJson.has("BestBid") && dshbtcJson.has("BestAsk")&& dshbtcJson.getJSONObject("BestBid").length() > 0 && dshbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/DSHBTC/BESTBIDASKORDERBOOK/HITBTC_DSHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}

			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "HITBTC_dshbtc data is not written to ADL");
			}

		}
		
	}
}

