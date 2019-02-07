package co.biz.liqui.sink;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.Core;
import com.microsoft.azure.datalake.store.OperationResponse;
import com.microsoft.azure.datalake.store.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

import co.biz.liqui.util.LiquiSingletonClass;


public class LiquiDshbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(LiquiDshbtcADLSink.class);


	@Override
	public void invoke(String DSHBTC) throws Exception {
		if(DSHBTC.toString().startsWith("{") && DSHBTC.toString().endsWith("}"))
		{
			JSONObject DSHBTCJson = new JSONObject((java.lang.String) DSHBTC);
			LiquiSingletonClass obj = LiquiSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (DSHBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(DSHBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIQUI/DSHBTC/RAWORDERBOOK/LIQUI_DSHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("MidPoint") && DSHBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIQUI/DSHBTC/MIDPOINT/LIQUI_DSHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIQUI/DSHBTC/TICKER/LIQUI_DSHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/LIQUI/DSHBTC/TRADEBOOK/LIQUI_DSHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("BestBid") && DSHBTCJson.has("BestAsk") && DSHBTCJson.getJSONObject("BestBid").length() > 0 && DSHBTCJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIQUI/DSHBTC/BESTBIDASKORDERBOOK/LIQUI_DSHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "LIQUI_DSHBTC data is not written to ADL");
			}

		}

	}
}
