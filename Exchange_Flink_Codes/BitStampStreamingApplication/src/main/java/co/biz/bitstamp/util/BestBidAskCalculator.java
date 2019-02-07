package co.biz.bitstamp.util;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Dhinesh Raja
 *
 */
public class BestBidAskCalculator implements Serializable {
	private final static Logger logger = LoggerFactory.getLogger(BestBidAskCalculator.class);
	private static final long serialVersionUID = 1L;

	public DataStream<String> calcBestBidAskStream(DataStream<String> rawOrderStream) {

		
		DataStream<String> bestBidAskorderStream = rawOrderStream.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String map(String rawOrderData) throws Exception {
				String bestBidAskdata = new JSONObject().toString();
				JSONObject rawJson = new JSONObject(rawOrderData);
				if (rawJson.has("Asks") && rawJson.has("Bids")) {
					JSONArray asksArray = rawJson.getJSONArray("Asks");
					JSONArray bidsArray = rawJson.getJSONArray("Bids");
					JSONObject bestBid = bidsArray.getJSONObject(0);
					JSONObject bestAsk = asksArray.getJSONObject(0);
					JSONObject bestBidAsk = new JSONObject();
					bestBidAsk.put("TimeStamp", rawJson.get("TimeStamp"));
					bestBidAsk.put("BestBid", bestBid);
					bestBidAsk.put("BestAsk", bestAsk);
					bestBidAskdata = bestBidAsk.toString();
				} 
				return bestBidAskdata;
			}
		});
		return bestBidAskorderStream;
	}

	
}
