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





public class BittrexXmrbtcADLSink<String> extends RichSinkFunction<String> {

	private static final long serialVersionUID = 1L;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BittrexXmrbtcADLSink.class);
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
	public void invoke(String XMRBTC) throws Exception {
		if(XMRBTC.toString().startsWith("{") && XMRBTC.toString().endsWith("}"))
		{
			JSONObject XMRBTCJson = new JSONObject((java.lang.String) XMRBTC);
			BittrexSingletonClass obj = BittrexSingletonClass.getInstance();
			ADLStoreClient client = obj.getADLStoreClient();
			byte[] myBuffer = (XMRBTC + "\n").getBytes();
			RequestOptions opts = new RequestOptions();
			opts.retryPolicy = new ExponentialBackoffPolicy();
			OperationResponse resp = new OperationResponse();

			if(XMRBTCJson.has("Snapshot"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XMRBTC/RAWORDERBOOK/BITTREX_XMRBTC_RAWORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("MidPoint"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XMRBTC/MIDPOINT/BITTREX_XMRBTC_MIDPOINT_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("TickerBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XMRBTC/TICKER/BITTREX_XMRBTC_TICKER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("TradeBook"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XMRBTC/TRADEBOOK/BITTREX_XMRBTC_TRADE_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}
			else if(XMRBTCJson.has("BestBid") && XMRBTCJson.has("BestAsk"))
			{
				Core.concurrentAppend("/EXCHANGE_DATA/BITTREX/XMRBTC/BESTBIDASKORDERBOOK/BITTREX_XMRBTC_BESTBIDASKORDER_"+new SimpleDateFormat("yyyy-MM-dd").format(new Date(System.currentTimeMillis()))+".json", myBuffer, 0, myBuffer.length, true, client, opts, resp);
			}		
			if (!resp.successful) {
				throw client.getExceptionFromResponse(resp, "BITTREX_XMRBTC data is not written to ADL");
			}

		}

	}
}
