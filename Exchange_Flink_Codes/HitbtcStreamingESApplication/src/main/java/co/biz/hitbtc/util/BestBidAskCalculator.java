package co.biz.hitbtc.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static java.util.Comparator.comparing;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
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
	public List<OrderData> bidOrderBook = new CopyOnWriteArrayList<OrderData>();
	public List<OrderData> askOrderBook = new CopyOnWriteArrayList<OrderData>();
	public  long sequenceNo = Long.MAX_VALUE;
	private 	List<List<OrderData>> orderBookSnap = new ArrayList<>();

	public DataStream<String> calcBestBidAskStream(DataStream<String> rawOrderStream, String currency) {

		DataStream<String> bestBidAskorderStream = rawOrderStream.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String map(String rawOrderData) throws Exception {
				String bestBidAskdata = new JSONObject().toString();
				JSONObject rawJson = new JSONObject(rawOrderData);
				String timeStamp = rawJson.getString("TimeStamp");
				if (rawJson.has("Snapshot")) {
					bidOrderBook.clear();
					askOrderBook.clear();
					JSONObject snapShotJObject = rawJson.getJSONObject("Snapshot");
					sequenceNo = rawJson.getLong("SequenceNumber");
					JSONArray bidArray = snapShotJObject.getJSONArray("Bids");
					JSONArray askArray = snapShotJObject.getJSONArray("Asks");
					String machineTime = snapShotJObject.getString("MachineTime");
					for (Object object : bidArray) {
						JSONObject json = (JSONObject) object;
						OrderData snapshotOrderObj = getOrderObject(json);
						bidOrderBook.add(snapshotOrderObj);
					}
					for (Object object : askArray) {
						JSONObject json = (JSONObject) object;
						OrderData snapshotOrderObj = getOrderObject(json);
						askOrderBook.add(snapshotOrderObj);
					}
					bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
				} else if (rawJson.has("Updates") && rawJson.getLong("SequenceNumber") > sequenceNo) {
					JSONArray updateArray = rawJson.getJSONArray("Updates");
					String machineTime = "";
					for (Object object : updateArray) {
						JSONObject json = (JSONObject) object;
						machineTime = json.getString("MachineTime");
						List<OrderData> removalList = new ArrayList<>();
						OrderData updateOrderObj = getOrderObject(json);
						if (updateOrderObj.getOrderSide().equals("bids")) {
							for (OrderData order : bidOrderBook) {
								if ((updateOrderObj.getQuantity().equals(0.0))
										&& (order.getPrice().equals(updateOrderObj.getPrice()))) {
									removalList.add(order);
									bidOrderBook.removeAll(removalList);
								} else if (!(updateOrderObj.getQuantity().equals(0.0))
										&& (order.getPrice().equals(updateOrderObj.getPrice()))) {
									removalList.add(order);
									bidOrderBook.removeAll(removalList);
									bidOrderBook.add(updateOrderObj);
								}
							}
							if (!(updateOrderObj.getQuantity().equals(0.0)) && (removalList.size() == 0)) {
								bidOrderBook.add(updateOrderObj);
							}
						} else if (updateOrderObj.getOrderSide().equals("asks")) {
							for (OrderData askorder : askOrderBook) {
								if ((updateOrderObj.getQuantity().equals(0.0))
										&& (askorder.getPrice().equals(updateOrderObj.getPrice()))) {
									removalList.add(askorder);
									askOrderBook.removeAll(removalList);
								} else if (!(updateOrderObj.getQuantity().equals(0.0))
										&& (askorder.getPrice().equals(updateOrderObj.getPrice()))) {
									removalList.add(askorder);
									askOrderBook.removeAll(removalList);
									askOrderBook.add(updateOrderObj);
								}
							}
							if (!(updateOrderObj.getQuantity().equals(0.0)) && (removalList.size() == 0)) {

								askOrderBook.add(updateOrderObj);
							}
						}
					}
					if (askOrderBook.size() > 0) {
						Comparator<OrderData> comparator = comparing(OrderData::getPrice, naturalOrder());
						Collections.sort(askOrderBook, comparator);
					}
					if (bidOrderBook.size() > 0) {
						Comparator<OrderData> comparator = comparing(OrderData::getPrice, reverseOrder());
						Collections.sort(bidOrderBook, comparator);
					}
					bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
				}
			
				Tuple2<List<CopyOnWriteArrayList<OrderData>>,Long> orderBookTuple = new Tuple2<>();
				 List<CopyOnWriteArrayList<OrderData>> orderBookSnap = new ArrayList<>();
				orderBookSnap.add((CopyOnWriteArrayList<co.biz.hitbtc.util.OrderData>) bidOrderBook);
				orderBookSnap.add((CopyOnWriteArrayList<co.biz.hitbtc.util.OrderData>) askOrderBook);
				orderBookTuple.f0 = orderBookSnap;
				orderBookTuple.f1 = sequenceNo;
				FileOutputStream fos = null;
		        ObjectOutputStream out = null;
		        try {
		            fos = new FileOutputStream("/home/bizruntime/state/hitbtcES"+currency+".txt",false);
		            out = new ObjectOutputStream(fos);
		            out.writeObject(orderBookTuple);

		            out.close();
		        } catch (Exception ex) {
		           logger.error(ex.getMessage());
		        }
				return bestBidAskdata;
			}
		});
		return bestBidAskorderStream;
	}
	
		
	private OrderData getOrderObject(JSONObject orderJson) {
		OrderData orderObj = new OrderData();
		orderObj.setExchangeName(orderJson.getString("ExchangeName"));
		orderObj.setCurrencyPair(orderJson.getString("CurrencyPair"));
		orderObj.setOrderSide(orderJson.getString("OrderSide"));
		orderObj.setTime(orderJson.getString("MachineTime"));
		orderObj.setPrice(orderJson.getDouble("Price"));
		orderObj.setQuantity(orderJson.getDouble("Quantity"));
		return orderObj;
	}

	private String getBestBidAsk(String machineTime, String timeStamp) {

		JSONObject bestBidAsk = new JSONObject();
		JSONObject bidObj = new JSONObject();
		JSONObject askObj = new JSONObject();
		if (bidOrderBook.size() > 0 && askOrderBook.size() > 0) {

			bidObj.put("ExchangeName", bidOrderBook.get(0).getExchangeName());
			bidObj.put("CurrencyPair", bidOrderBook.get(0).getCurrencyPair());
			bidObj.put("MachineTime", machineTime);
			bidObj.put("OrderSide", bidOrderBook.get(0).getOrderSide());
			bidObj.put("Price", bidOrderBook.get(0).getPrice());
			bidObj.put("Quantity", bidOrderBook.get(0).getQuantity());

			askObj.put("ExchangeName", askOrderBook.get(0).getExchangeName());
			askObj.put("CurrencyPair", askOrderBook.get(0).getCurrencyPair());
			askObj.put("MachineTime", machineTime);
			askObj.put("OrderSide", askOrderBook.get(0).getOrderSide());
			askObj.put("Price", askOrderBook.get(0).getPrice());
			askObj.put("Quantity", askOrderBook.get(0).getQuantity());
		}

		bestBidAsk.put("TimeStamp", timeStamp);
		bestBidAsk.put("BestBid", bidObj);
		bestBidAsk.put("BestAsk", askObj);

		return bestBidAsk.toString();
	}

	
	
}
