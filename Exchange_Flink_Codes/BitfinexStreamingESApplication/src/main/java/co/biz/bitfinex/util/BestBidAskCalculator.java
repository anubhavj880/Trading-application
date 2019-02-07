package co.biz.bitfinex.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import static java.util.Comparator.comparing;
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
	
	public  List<OrderData> bidOrderBook = new CopyOnWriteArrayList<OrderData>();
	public  List<OrderData> askOrderBook = new CopyOnWriteArrayList<OrderData>();

	private static Comparator<OrderData> bidComparator = comparing(OrderData::getPrice, reverseOrder());
	private static Comparator<OrderData> askComparator = comparing(OrderData::getPrice, naturalOrder());
	private String currency = null;

	public BestBidAskCalculator(String currency) {
		 this.currency = currency;
	}


	public DataStream<String> calcBestBidAskStream(DataStream<String> rawOrderStream) {
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
					JSONArray nodes = rawJson.getJSONArray("Snapshot");
					String machineTime = "";
					for (int i = 1; i < nodes.length(); i++) {
						JSONObject json = (JSONObject) nodes.get(i);
						OrderData snapshotOrderObj = getOrderObject(json);
						if (snapshotOrderObj.getOrderSide().equals("bids")) {

							bidOrderBook.add(snapshotOrderObj);
							

						} else if (snapshotOrderObj.getOrderSide().equals("asks")) {

							askOrderBook.add(snapshotOrderObj);

							
						}
						machineTime = json.getString("MachineTime");
					}
					bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
					List<List<OrderData>> orderBookSnap = new ArrayList<List<OrderData>>();
					orderBookSnap.add(bidOrderBook);
					orderBookSnap.add(askOrderBook);
					FileOutputStream fos = null;
			        ObjectOutputStream out = null;
			        try {
			            fos = new FileOutputStream("/home/bizruntime/state/bitfinexES"+currency+".txt",false);
			            out = new ObjectOutputStream(fos);
			            out.writeObject(orderBookSnap);
			            out.flush();
			            out.close();
			           
			        } catch (Exception ex) {
			           logger.error(ex.getMessage());
			        }
				} else if (rawJson.has("Updates") && bidOrderBook.size() > 0 && askOrderBook.size() > 0) {
					JSONArray updateArray = rawJson.getJSONArray("Updates");
					JSONObject updateJson = updateArray.getJSONObject(1);
					List<OrderData> removalList = new ArrayList<>();
					OrderData updateOrderObj = getOrderObject(updateJson);
					if (updateOrderObj.getOrderSide().equals("bids")) {
						if (updateOrderObj.getPrice().equals(0.0)) {

							for (OrderData order : bidOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							bidOrderBook.removeAll(removalList);
							Collections.sort(bidOrderBook, bidComparator);
							bestBidAskdata = getBestBidAsk(updateOrderObj.getTime(), timeStamp);
							

						} else if (!(updateOrderObj.getPrice().equals(0.0))) {
							for (OrderData order : bidOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							if ((removalList.size() > 0)) {
								bidOrderBook.removeAll(removalList);
								bidOrderBook.add(updateOrderObj);
							} else if ((removalList.size() == 0)) {
								bidOrderBook.add(updateOrderObj);
							}
							Collections.sort(bidOrderBook, bidComparator);
							bestBidAskdata = getBestBidAsk(updateOrderObj.getTime(), timeStamp);
						}
					}
					else if (updateOrderObj.getOrderSide().equals("asks")) {
						if (updateOrderObj.getPrice().equals(0.0)) {
							
							for (OrderData order : askOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							askOrderBook.removeAll(removalList);
							Collections.sort(askOrderBook, askComparator);
							bestBidAskdata = getBestBidAsk(updateOrderObj.getTime(), timeStamp);
							
						} else if (!(updateOrderObj.getPrice().equals(0.0))) {
							for (OrderData order : askOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							if ((removalList.size() > 0)) {
								askOrderBook.removeAll(removalList);
								askOrderBook.add(updateOrderObj);
							} else if ((removalList.size() == 0)) {
								askOrderBook.add(updateOrderObj);
							}
							Collections.sort(askOrderBook, askComparator);
							bestBidAskdata = getBestBidAsk(updateOrderObj.getTime(), timeStamp);
				
						}
					}
					List<List<OrderData>> orderBookSnap = new ArrayList<List<OrderData>>();
					orderBookSnap.add(bidOrderBook);
					orderBookSnap.add(askOrderBook);
					FileOutputStream fos = null;
			        ObjectOutputStream out = null;
			        try {
			            fos = new FileOutputStream("/home/bizruntime/state/bitfinexES"+currency+".txt",false);
			            out = new ObjectOutputStream(fos);
			            out.writeObject(orderBookSnap);
			            out.flush();
			            out.close();
			           
			        } catch (Exception ex) {
			           logger.error(ex.getMessage());
			        }
				}
				 
				 return bestBidAskdata;
			}
		});
		return bestBidAskorderStream;
	}

	
	public OrderData getOrderObject(JSONObject orderJson) {
		OrderData orderObj = new OrderData();
		orderObj.setExchangeName(orderJson.getString("ExchangeName"));
		orderObj.setCurrencyPair(orderJson.getString("CurrencyPair"));
		orderObj.setOrderSide(orderJson.getString("OrderSide"));
		orderObj.setOrderId(orderJson.getLong("OrderId"));
		orderObj.setTime(orderJson.getString("MachineTime"));
		orderObj.setPrice(orderJson.getDouble("Price"));
		orderObj.setQuantity(orderJson.getDouble("Quantity"));
		return orderObj;
	}

	public String getBestBidAsk(String machineTime, String timeStamp) {
		JSONObject bestBidAsk = new JSONObject();
		JSONObject bidObj = new JSONObject();
		JSONObject askObj = new JSONObject();
		if (bidOrderBook.size() > 0 && askOrderBook.size() > 0) {
			bidObj.put("ExchangeName", bidOrderBook.get(0).getExchangeName());
			bidObj.put("CurrencyPair", bidOrderBook.get(0).getCurrencyPair());
			bidObj.put("MachineTime", machineTime);
			bidObj.put("OrderSide", bidOrderBook.get(0).getOrderSide());
			bidObj.put("OrderId", bidOrderBook.get(0).getOrderId());
			bidObj.put("Price", bidOrderBook.get(0).getPrice());
			bidObj.put("Quantity", bidOrderBook.get(0).getQuantity());

			askObj.put("ExchangeName", askOrderBook.get(0).getExchangeName());
			askObj.put("CurrencyPair", askOrderBook.get(0).getCurrencyPair());
			askObj.put("MachineTime", machineTime);
			askObj.put("OrderSide", askOrderBook.get(0).getOrderSide());
			askObj.put("OrderId", askOrderBook.get(0).getOrderId());
			askObj.put("Price", askOrderBook.get(0).getPrice());
			askObj.put("Quantity", askOrderBook.get(0).getQuantity());
		}
		bestBidAsk.put("TimeStamp", timeStamp);
		bestBidAsk.put("BestBid", bidObj);
		bestBidAsk.put("BestAsk", askObj);
		return bestBidAsk.toString();
	}
}
