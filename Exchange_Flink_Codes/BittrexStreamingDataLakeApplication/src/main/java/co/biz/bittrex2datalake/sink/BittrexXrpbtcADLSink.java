package co.biz.bittrex2datalake.sink;

import java.io.IOException;
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

import co.biz.bittrex2datalake.util.BittrexSingletonClass;





/**
 * 
 * @author Dhinesh Raja
 *
 */
@SuppressWarnings("hiding")
public class BittrexXrpbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BittrexXrpbtcADLSink.class);
	private static ADLStoreClient client = null;
	static 
	{
		try {
			 client = BittrexSingletonClass.getInstance().getADLStoreClient();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	

	@Override
	public void invoke(String XRPBTC) throws Exception {
		if(XRPBTC.toString().startsWith("{") && XRPBTC.toString().endsWith("}"))
		{
			JSONObject XRPBTCJson = new JSONObject((java.lang.String) XRPBTC);
			
			byte[] myBuffer = (XRPBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(XRPBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XRPBTC/RAWORDERBOOK/BITTREX_XRPBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("MidPoint"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XRPBTC/MIDPOINT/BITTREX_XRPBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XRPBTC/TICKER/BITTREX_XRPBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XRPBTC/TRADEBOOK/BITTREX_XRPBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XRPBTCJson.has("BestBid") && XRPBTCJson.has("BestAsk"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XRPBTC/BESTBIDASKORDERBOOK/BITTREX_XRPBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITTREX_XRPBTC data is not written to ADL");
			}

		}

	}
}
