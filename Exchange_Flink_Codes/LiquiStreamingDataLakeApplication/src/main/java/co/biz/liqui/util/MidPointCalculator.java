package co.biz.liqui.util;

import java.io.Serializable;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author Dhinesh Raja
 *
 */
public class MidPointCalculator implements Serializable{
	private static final long serialVersionUID = 1L;

	private final static Logger logger = LoggerFactory.getLogger(MidPointCalculator.class);
	public DataStream<String> calculateMidPoint(DataStream<String> bestBidAsk) {
		DataStream<String> MidStream = bestBidAsk.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			@Override
			public String map(String bestBidAskData) throws Exception {
				JSONObject rawJson = new JSONObject(bestBidAskData);
				String midpointString = new JSONObject().toString();
				if (rawJson.has("BestBid") && rawJson.has("BestAsk") && rawJson.getJSONObject("BestBid").length() != 0) {
					JSONObject bestBidJson = rawJson.getJSONObject("BestBid");
					JSONObject bestAskJson = rawJson.getJSONObject("BestAsk");
					Double midpoint = (bestBidJson.getDouble("Price") + bestAskJson.getDouble("Price")) / 2;
					JSONObject midpointJson = new JSONObject();
					midpointJson.put("ExchangeName", bestBidJson.get("ExchangeName"));
					midpointJson.put("CurrencyPair", bestBidJson.get("CurrencyPair"));
					midpointJson.put("MachineTime", bestBidJson.get("MachineTime"));
					midpointJson.put("MidPointData", midpoint);
					JSONObject midJson = new JSONObject();
					midJson.put("TimeStamp", rawJson.get("TimeStamp"));
					midJson.put("MidPoint", midpointJson);
					midpointString = midJson.toString();
				}

				return midpointString;
			}
		});
		return MidStream;
		
	}
	
}
