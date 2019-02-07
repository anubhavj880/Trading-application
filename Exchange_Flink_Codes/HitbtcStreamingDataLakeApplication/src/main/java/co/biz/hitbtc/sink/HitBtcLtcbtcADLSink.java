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



public class HitBtcLtcbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(HitBtcLtcbtcADLSink.class);


	@Override
	public void invoke(String ltcbtc) throws Exception {

		if(ltcbtc.toString().startsWith("{") && ltcbtc.toString().endsWith("}"))
		{
			JSONObject ltcbtcJson = new JSONObject((java.lang.String) ltcbtc);
			HitBtcSingletonClass obj = HitBtcSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (ltcbtc + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(ltcbtcJson.has("Snapshot")  || ltcbtcJson.has("Updates"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/LTCBTC/RAWORDERBOOK/HITBTC_LTCBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ltcbtcJson.has("MidPoint")&& ltcbtcJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/LTCBTC/MIDPOINT/HITBTC_LTCBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ltcbtcJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/LTCBTC/TICKER/HITBTC_LTCBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ltcbtcJson.has("TradeBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/HITBTC/LTCBTC/TRADEBOOK/HITBTC_LTCBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(ltcbtcJson.has("BestBid") && ltcbtcJson.has("BestAsk")&& ltcbtcJson.getJSONObject("BestBid").length() > 0 && ltcbtcJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/HITBTC/LTCBTC/BESTBIDASKORDERBOOK/HITBTC_LTCBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}

			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "HITBTC_ltcbtc data is not written to ADL");
			}

		}
		
	}
}
