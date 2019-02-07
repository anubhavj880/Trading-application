package co.biz.livecoin.sink;

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

import co.biz.livecoin.util.LiveCoinSingletonClass;



public class LiveCoinLtcbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(LiveCoinLtcbtcADLSink.class);

	private static final java.lang.String LIVECOIN_LTCBTC_RAWORDER = "/EXCHANGE_DATA/LIVECOIN/LTCBTC/RAWORDERBOOK/LIVECOIN_LTCBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String LIVECOIN_LTCBTC_BESTBIDASKORDER = "/EXCHANGE_DATA/LIVECOIN/LTCBTC/BESTBIDASKORDERBOOK/LIVECOIN_LTCBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String LIVECOIN_LTCBTC_TRADE = "/EXCHANGE_DATA/LIVECOIN/LTCBTC/TRADEBOOK/LIVECOIN_LTCBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String LIVECOIN_LTCBTC_TICKER = "/EXCHANGE_DATA/LIVECOIN/LTCBTC/TICKER/LIVECOIN_LTCBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String LIVECOIN_LTCBTC_MIDPOINT = "/EXCHANGE_DATA/LIVECOIN/LTCBTC/MIDPOINT/LIVECOIN_LTCBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";

	@Override
	public void invoke(String LTCBTC) throws Exception {
		if(LTCBTC.toString().startsWith("{") && LTCBTC.toString().endsWith("}"))
		{
			JSONObject LTCBTCJson = new JSONObject((java.lang.String) LTCBTC);
			LiveCoinSingletonClass obj = LiveCoinSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (LTCBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(LTCBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIVECOIN/LTCBTC/RAWORDERBOOK/LIVECOIN_LTCBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("MidPoint") && LTCBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/LIVECOIN/LTCBTC/MIDPOINT/LIVECOIN_LTCBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/LIVECOIN/LTCBTC/TICKER/LIVECOIN_LTCBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIVECOIN/LTCBTC/TRADEBOOK/LIVECOIN_LTCBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("BestBid") && LTCBTCJson.has("BestAsk") && LTCBTCJson.getJSONObject("BestBid").length() > 0 && LTCBTCJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/LIVECOIN/LTCBTC/BESTBIDASKORDERBOOK/LIVECOIN_LTCBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "LIVECOIN_LTCBTC data is not written to ADL");
			}

		}

	}
}
