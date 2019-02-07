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
public class PoloniexDshbtcADLSink<String> extends RichSinkFunction<String> {

	
	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(PoloniexDshbtcADLSink.class);
	
	@Override
	public void invoke(String DSHBTC) throws Exception {
		if(DSHBTC.toString().startsWith("{") && DSHBTC.toString().endsWith("}"))
		{
			JSONObject DSHBTCJson = new JSONObject((java.lang.String) DSHBTC);
			PoloniexSingletonClass obj = PoloniexSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (DSHBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(DSHBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/DSHBTC/RAWORDERBOOK/POLONIEX_DSHBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("MidPoint") && DSHBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/DSHBTC/MIDPOINT/POLONIEX_DSHBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/DSHBTC/TICKER/POLONIEX_DSHBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/POLONIEX/DSHBTC/TRADEBOOK/POLONIEX_DSHBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(DSHBTCJson.has("BestBid") && DSHBTCJson.has("BestAsk") && DSHBTCJson.getJSONObject("BestBid").length() > 0 && DSHBTCJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/DSHBTC/BESTBIDASKORDERBOOK/POLONIEX_DSHBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "POLONIEX_DSHBTC data is not written to ADL");
			}

		}

	}
}
