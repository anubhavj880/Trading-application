package co.biz.bitstamp.sink;

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

import co.biz.bitstamp.util.BitStampSingletonClass;



/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class BitStampXrpbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitStampXrpbtcADLSink.class);

	private static final java.lang.String BITSTAMP_XRPBTC_RAWORDER = "/EXCHANGE_DATA/BITSTAMP/XRPBTC/RAWORDERBOOK/BITSTAMP_XRPBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String BITSTAMP_XRPBTC_BESTBIDASKORDER = "/EXCHANGE_DATA/BITSTAMP/XRPBTC/BESTBIDASKORDERBOOK/BITSTAMP_XRPBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String BITSTAMP_XRPBTC_TRADE = "/EXCHANGE_DATA/BITSTAMP/XRPBTC/TRADEBOOK/BITSTAMP_XRPBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String BITSTAMP_XRPBTC_TICKER = "/EXCHANGE_DATA/BITSTAMP/XRPBTC/TICKER/BITSTAMP_XRPBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";
	private static final java.lang.String BITSTAMP_XRPBTC_MIDPOINT = "/EXCHANGE_DATA/BITSTAMP/XRPBTC/MIDPOINT/BITSTAMP_XRPBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json";

	@Override
	public void invoke(String XRPBTC) throws Exception {
		if(XRPBTC.toString().startsWith("{") && XRPBTC.toString().endsWith("}"))
		{
			JSONObject XRPBTCJson = new JSONObject((java.lang.String) XRPBTC);
			BitStampSingletonClass obj = BitStampSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (XRPBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if((XRPBTCJson.has("Asks")) || (XRPBTCJson.has("Bids")))
			{
				Core.concurrentAppend(BITSTAMP_XRPBTC_RAWORDER, myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("MidPoint"))
			{
				Core.concurrentAppend(BITSTAMP_XRPBTC_MIDPOINT, myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend(BITSTAMP_XRPBTC_TICKER, myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend(BITSTAMP_XRPBTC_TRADE, myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("BestBid") && XRPBTCJson.has("BestAsk"))
			{
				Core.concurrentAppend(BITSTAMP_XRPBTC_BESTBIDASKORDER, myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITSTAMP_XRPBTC data is not written to ADL");
			}

		}

	}
}
