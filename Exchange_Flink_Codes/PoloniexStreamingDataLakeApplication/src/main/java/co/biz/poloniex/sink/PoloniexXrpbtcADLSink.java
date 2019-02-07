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
public class PoloniexXrpbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(PoloniexXrpbtcADLSink.class);
	private static final java.lang.String POLONIEX_XRPBTC_RAWORDER = "/EXCHANGE_DATA/POLONIEX/XRPBTC/RAWORDERBOOK/POLONIEX_XRPBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String POLONIEX_XRPBTC_BESTBIDASKORDER = "/EXCHANGE_DATA/POLONIEX/XRPBTC/BESTBIDASKORDERBOOK/POLONIEX_XRPBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String POLONIEX_XRPBTC_TRADE = "/EXCHANGE_DATA/POLONIEX/XRPBTC/TRADEBOOK/POLONIEX_XRPBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String POLONIEX_XRPBTC_TICKER = "/EXCHANGE_DATA/POLONIEX/XRPBTC/TICKER/POLONIEX_XRPBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String POLONIEX_XRPBTC_MIDPOINT = "/EXCHANGE_DATA/POLONIEX/XRPBTC/MIDPOINT/POLONIEX_XRPBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";

	@Override
	public void invoke(String XRPBTC) throws Exception {
		if(XRPBTC.toString().startsWith("{") && XRPBTC.toString().endsWith("}"))
		{
			JSONObject XRPBTCJson = new JSONObject((java.lang.String) XRPBTC);
			PoloniexSingletonClass obj = PoloniexSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (XRPBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(XRPBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/XRPBTC/RAWORDERBOOK/POLONIEX_XRPBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("MidPoint") && XRPBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/POLONIEX/XRPBTC/MIDPOINT/POLONIEX_XRPBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/POLONIEX/XRPBTC/TICKER/POLONIEX_XRPBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/XRPBTC/TRADEBOOK/POLONIEX_XRPBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("BestBid") && XRPBTCJson.has("BestAsk") && XRPBTCJson.getJSONObject("BestBid").length() > 0 && XRPBTCJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/POLONIEX/XRPBTC/BESTBIDASKORDERBOOK/POLONIEX_XRPBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "POLONIEX_XRPBTC data is not written to ADL");
			}

		}

	}
}
