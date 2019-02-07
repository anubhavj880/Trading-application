package co.biz.therock.sink;

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

import co.biz.therock.util.TheRockSingletonClass;



/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class TheRockXrpBtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(TheRockXrpBtcADLSink.class);

	@Override
	public void invoke(String XRPBTC) throws Exception {
		if(XRPBTC.toString().startsWith("{") && XRPBTC.toString().endsWith("}"))
		{
			JSONObject XRPBTCJson = new JSONObject((java.lang.String) XRPBTC);
			TheRockSingletonClass obj = TheRockSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (XRPBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if((XRPBTCJson.has("Asks")) && (XRPBTCJson.has("Bids")))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/THEROCK/XRPBTC/RAWORDERBOOK/THEROCK_XRPBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("MidPoint") && XRPBTCJson.getJSONObject("MidPoint").length() > 0)
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/THEROCK/XRPBTC/MIDPOINT/THEROCK_XRPBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend( "/EXCHANGE_DATA/THEROCK/XRPBTC/TICKER/THEROCK_XRPBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/THEROCK/XRPBTC/TRADEBOOK/THEROCK_XRPBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("BestBid") && XRPBTCJson.has("BestAsk") && XRPBTCJson.getJSONObject("BestBid").length() > 0 && XRPBTCJson.getJSONObject("BestAsk").length() > 0)
			{
				Core.concurrentAppend("/EXCHANGE_DATA/THEROCK/XRPBTC/BESTBIDASKORDERBOOK/THEROCK_XRPBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "THEROCK_XRPBTC data is not written to ADL");
			}

		}

	}
}
