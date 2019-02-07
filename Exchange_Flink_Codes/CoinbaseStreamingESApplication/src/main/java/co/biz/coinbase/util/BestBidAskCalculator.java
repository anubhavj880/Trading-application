package co.biz.coinbase.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static java.util.Comparator.comparing;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.hadoop.shaded.org.apache.http.HttpResponse;
import org.apache.flink.hadoop.shaded.org.apache.http.client.HttpClient;
import org.apache.flink.hadoop.shaded.org.apache.http.client.methods.HttpGet;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.DefaultHttpClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONException;
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
	private static Comparator<OrderData> bidComparator = comparing(OrderData::getPrice, reverseOrder());
	private static Comparator<OrderData> askComparator = comparing(OrderData::getPrice, naturalOrder());
	public long randomNo = 0L;
	public long sequenceNo = 0L;
	private List<JSONObject> updateJson = new CopyOnWriteArrayList<JSONObject>();
	
	
	public DataStream<String> calcBestBidAskStream(DataStream<String> rawOrderStream,String currency) {

		DataStream<String> bestBidAskorderStream = rawOrderStream.map(new MapFunction<String, String>() {

			@Override
			public String map(String rawOrderData) throws Exception {
				String bestBidAskdata = new JSONObject().toString();
				JSONObject rawJson = new JSONObject(rawOrderData);
				String timeStamp = rawJson.getString("TimeStamp");
				if (rawJson.has("Snapshot")) {
					bidOrderBook.clear();
					askOrderBook.clear();
					JSONObject snapShotJObject = rawJson.getJSONObject("Snapshot");
					JSONArray bidArray = snapShotJObject.getJSONArray("Bids");
					JSONArray askArray = snapShotJObject.getJSONArray("Asks");
					String machineTime = snapShotJObject.getString("MachineTime");
					for (int i = 0; i < 100; i++) {
						JSONObject json = (JSONObject) bidArray.get(i);
						OrderData snapshotOrderObj = getOrderObject(json);
						bidOrderBook.add(snapshotOrderObj);
					}
					for (int i = 0; i < 100; i++) {
						JSONObject json = (JSONObject) askArray.get(i);
						OrderData snapshotOrderObj = getOrderObject(json);
						askOrderBook.add(snapshotOrderObj);
					}
					sequenceNo = rawJson.getLong("SequenceNumber");
					randomNo = rawJson.getLong("RandomNumber");
					bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
				} else if (rawJson.has("Updates") && ((sequenceNo == 0L)|| (randomNo != rawJson.getLong("RandomNumber")))) {
					updateJson.add(rawJson);
				}
				
				  else if (rawJson.has("Updates") && (rawJson.getLong("SequenceNumber") > sequenceNo) && (randomNo == rawJson.getLong("RandomNumber"))) {
					if (!updateJson.isEmpty()) {
						for (JSONObject updateOrder : updateJson) {
							if (updateOrder.getLong("SequenceNumber") > sequenceNo) {
								JSONObject updateJobject = updateOrder.getJSONObject("Updates");
								String machineTime = updateJobject.getString("MachineTime");
								List<OrderData> removalList = new ArrayList<>();
								OrderData updateOrderObj = getOrderObject(updateJobject);
								if (updateOrderObj.getOrderSide().equals("bids")) {

									if ((updateOrderObj.getUpdateType().equals("done"))) {
										for (OrderData order : bidOrderBook) {
											if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
												removalList.add(order);
											}
										}
										bidOrderBook.removeAll(removalList);
										Collections.sort(bidOrderBook, bidComparator);
									} else if ((updateOrderObj.getUpdateType().equals("change"))) {
										for (OrderData order : bidOrderBook) {
											if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
												removalList.add(order);
											}
										}
										bidOrderBook.removeAll(removalList);
										if (removalList.size() > 0) {
											bidOrderBook.add(updateOrderObj);
										}
										Collections.sort(bidOrderBook, bidComparator);
									} else if ((updateOrderObj.getUpdateType().equals("match"))) {
										for (OrderData order : bidOrderBook) {
											if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
												removalList.add(order);
											}
										}
										bidOrderBook.removeAll(removalList);
										Collections.sort(bidOrderBook, bidComparator);
									} else if ((updateOrderObj.getUpdateType().equals("open"))) {

										bidOrderBook.add(updateOrderObj);
										Collections.sort(bidOrderBook, bidComparator);

									}

									
									bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
								} else if (updateOrderObj.getOrderSide().equals("asks")) {
									if ((updateOrderObj.getUpdateType().equals("done"))) {
										for (OrderData order : askOrderBook) {
											if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
												removalList.add(order);
											}
										}
										askOrderBook.removeAll(removalList);
										Collections.sort(askOrderBook, askComparator);
									} else if ((updateOrderObj.getUpdateType().equals("change"))) {
										for (OrderData order : askOrderBook) {
											if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
												removalList.add(order);
											}
										}
										askOrderBook.removeAll(removalList);
										if (removalList.size() > 0) {
											askOrderBook.add(updateOrderObj);
										}
										Collections.sort(askOrderBook, askComparator);
									} else if ((updateOrderObj.getUpdateType().equals("match"))) {
										for (OrderData order : askOrderBook) {
											if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
												removalList.add(order);
											}
										}
										askOrderBook.removeAll(removalList);
										Collections.sort(askOrderBook, askComparator);
									} else if ((updateOrderObj.getUpdateType().equals("open"))) {

										askOrderBook.add(updateOrderObj);
										Collections.sort(askOrderBook, askComparator);

									}
									
									bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
								}
							}

						}
						sequenceNo = updateJson.get(updateJson.size() - 1).getLong("SequenceNumber");

						updateJson.clear();
					}
					JSONObject updateJobject = rawJson.getJSONObject("Updates");
					String machineTime = updateJobject.getString("MachineTime");
					List<OrderData> removalList = new ArrayList<>();
					OrderData updateOrderObj = getOrderObject(updateJobject);
					if (updateOrderObj.getOrderSide().equals("bids")) {
						if ((updateOrderObj.getUpdateType().equals("done"))) {
							for (OrderData order : bidOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							bidOrderBook.removeAll(removalList);
							Collections.sort(bidOrderBook, bidComparator);
						} else if ((updateOrderObj.getUpdateType().equals("change"))) {
							for (OrderData order : bidOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							bidOrderBook.removeAll(removalList);
							if (removalList.size() > 0) {
								bidOrderBook.add(updateOrderObj);
							}
							Collections.sort(bidOrderBook, bidComparator);
						} else if ((updateOrderObj.getUpdateType().equals("match"))) {
							for (OrderData order : bidOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							bidOrderBook.removeAll(removalList);
							Collections.sort(bidOrderBook, bidComparator);
						} else if ((updateOrderObj.getUpdateType().equals("open"))) {

							bidOrderBook.add(updateOrderObj);
							Collections.sort(bidOrderBook, bidComparator);

						}

						
						bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
					} else if (updateOrderObj.getOrderSide().equals("asks")) {
						if ((updateOrderObj.getUpdateType().equals("done"))) {
							for (OrderData order : askOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							askOrderBook.removeAll(removalList);
							Collections.sort(askOrderBook, askComparator);
						} else if ((updateOrderObj.getUpdateType().equals("change"))) {
							for (OrderData order : askOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							askOrderBook.removeAll(removalList);
							if (removalList.size() > 0) {
								askOrderBook.add(updateOrderObj);
							}
							Collections.sort(askOrderBook, askComparator);
						} else if ((updateOrderObj.getUpdateType().equals("match"))) {
							for (OrderData order : askOrderBook) {
								if (order.getOrderId().equals(updateOrderObj.getOrderId())) {
									removalList.add(order);
								}
							}
							askOrderBook.removeAll(removalList);
							Collections.sort(askOrderBook, askComparator);
						} else if ((updateOrderObj.getUpdateType().equals("open"))) {

							askOrderBook.add(updateOrderObj);
							Collections.sort(askOrderBook, askComparator);

						}

						
						bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
					}
					sequenceNo = rawJson.getLong("SequenceNumber");
					randomNo = rawJson.getLong("RandomNumber");
				}
				 Tuple3<List<CopyOnWriteArrayList<OrderData>>,Long,Long> orderBookTuple = new Tuple3<>();
				 List<CopyOnWriteArrayList<OrderData>> orderBookSnap = new ArrayList<>();
				orderBookSnap.add( (CopyOnWriteArrayList<OrderData>) bidOrderBook);
				orderBookSnap.add((CopyOnWriteArrayList<OrderData>) askOrderBook);
				orderBookTuple.f0 = orderBookSnap;
				orderBookTuple.f1 = sequenceNo;
				orderBookTuple.f2 = randomNo;
				FileOutputStream fos = null;
		        ObjectOutputStream out = null;
		        try {
		            fos = new FileOutputStream("/home/bizruntime/state/coinbaseES"+currency+".txt",false);
		            out = new ObjectOutputStream(fos);
		            out.writeObject(orderBookTuple);
		            out.flush();
		            out.close();
		            orderBookTuple = null;
		            orderBookSnap = null;
		           
		        } catch (Exception ex) {
		            ex.printStackTrace();
		        }
				return bestBidAskdata;
			}
		});
		return bestBidAskorderStream;
	}

	private OrderData getOrderObject(JSONObject orderJson) {
		OrderData orderObj = new OrderData();
		orderObj.setUpdateType(orderJson.getString("UpdateType"));
		orderObj.setExchangeName(orderJson.getString("ExchangeName"));
		orderObj.setCurrencyPair(orderJson.getString("CurrencyPair"));
		orderObj.setOrderSide(orderJson.getString("OrderSide"));
		orderObj.setOrderId(orderJson.getString("OrderId"));
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
