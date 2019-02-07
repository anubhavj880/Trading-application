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





public class BittrexLtcbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BittrexLtcbtcADLSink.class);
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
	public void invoke(String LTCBTC) throws Exception {
		if(LTCBTC.toString().startsWith("{") && LTCBTC.toString().endsWith("}"))
		{
			JSONObject LTCBTCJson = new JSONObject((java.lang.String) LTCBTC);
			
			byte[] myBuffer = (LTCBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(LTCBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/LTCBTC/RAWORDERBOOK/BITTREX_LTCBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("MidPoint"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/LTCBTC/MIDPOINT/BITTREX_LTCBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/LTCBTC/TICKER/BITTREX_LTCBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/LTCBTC/TRADEBOOK/BITTREX_LTCBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(LTCBTCJson.has("BestBid") && LTCBTCJson.has("BestAsk"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/LTCBTC/BESTBIDASKORDERBOOK/BITTREX_LTCBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITTREX_LTCBTC data is not written to ADL");
			}

		}

	}
}
