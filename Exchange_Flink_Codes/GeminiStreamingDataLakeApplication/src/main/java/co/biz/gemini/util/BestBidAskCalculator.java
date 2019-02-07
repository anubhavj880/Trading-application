package co.biz.gemini.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.reverseOrder;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import static java.util.Comparator.comparing;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.Core;
import com.microsoft.azure.datalake.store.OperationResponse;
import com.microsoft.azure.datalake.store.RequestOptions;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;


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
	private List<List<OrderData>> orderBookSnap = new ArrayList<>();
	
	
	

	public DataStream<String> calcBestBidAskStream(DataStream<String> rawOrderStream,String currency) {
		DataStream<String> bestBidAskorderStream = rawOrderStream.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = 1L;

			public String map(String rawOrderData) throws Exception {
				String bestBidAskdata = new JSONObject().toString();
				JSONObject rawJson = new JSONObject(rawOrderData);
				String timeStamp = rawJson.getString("TimeStamp");
				String machineTime = "";
				if (rawJson.has("Snapshot")) {
					bidOrderBook.clear();
					askOrderBook.clear();
					machineTime = rawJson.getJSONObject("Snapshot").getString("MachineTime");
					JSONArray bidArray = rawJson.getJSONObject("Snapshot").getJSONArray("Bids");
					JSONArray askArray = rawJson.getJSONObject("Snapshot").getJSONArray("Asks");
					for (Object object : bidArray) {
						OrderData snapshotBidOrder = getOrderObject((JSONObject) object);
						bidOrderBook.add(snapshotBidOrder);
					}
					for (Object object : askArray) {
						OrderData snapshotAskOrder = getOrderObject((JSONObject) object);
						askOrderBook.add(snapshotAskOrder);
					}
					Collections.sort(bidOrderBook, bidComparator);
					Collections.sort(askOrderBook, askComparator);
					bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
				} else if (rawJson.has("Updates") && bidOrderBook.size() > 0 && askOrderBook.size() > 0) {
					JSONObject updateJson = rawJson.getJSONObject("Updates");
					OrderData updateOrderObj = getOrderObject(updateJson);
					machineTime = rawJson.getJSONObject("Updates").getString("MachineTime");
					List<OrderData> removalList = new ArrayList<>();
					if (updateOrderObj.getOrderSide().equals("bids")) {
						
					
						
						if (updateOrderObj.getUpdateType().equals("place")) {
							for (OrderData orderData : bidOrderBook) {
								if(orderData.getPrice().equals(updateOrderObj.getPrice()))
								{
									removalList.add(orderData);
								}
							}
							
							bidOrderBook.removeAll(removalList);
							bidOrderBook.add(updateOrderObj);
							Collections.sort(bidOrderBook, bidComparator);
							bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
							
						} else if (updateOrderObj.getUpdateType().equals("cancel")) {
							for (OrderData orderData : bidOrderBook) {
								if(orderData.getPrice().equals(updateOrderObj.getPrice()))
								{
									removalList.add(orderData);
								}
							}
							bidOrderBook.removeAll(removalList);
													
							
							if (!(updateOrderObj.getQuantity().equals(0.0))) {
								bidOrderBook.add(updateOrderObj);
								
							}
							Collections.sort(bidOrderBook, bidComparator);
							bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
							
						} else if ((updateOrderObj.getUpdateType().equals("trade"))) {
							for (OrderData orderData : bidOrderBook) {
								if(orderData.getPrice().equals(updateOrderObj.getPrice()))
								{
									removalList.add(orderData);
								}
							}
							bidOrderBook.removeAll(removalList);
							
							
							if (!(updateOrderObj.getQuantity().equals(0.0))) {
								bidOrderBook.add(updateOrderObj);
								
							}
							Collections.sort(bidOrderBook, bidComparator);
							bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
							
						}
						
					} else if (updateOrderObj.getOrderSide().equals("asks")) {
						
						
						if (updateOrderObj.getUpdateType().equals("place")) {
							for (OrderData orderData : askOrderBook) {
								if(orderData.getPrice().equals(updateOrderObj.getPrice()))
								{
									removalList.add(orderData);
								}
							}
							
							askOrderBook.removeAll(removalList);
							askOrderBook.add(updateOrderObj);
							Collections.sort(askOrderBook, askComparator);
							bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
							
						} else if (updateOrderObj.getUpdateType().equals("cancel")) {
							for (OrderData orderData : askOrderBook) {
								if(orderData.getPrice().equals(updateOrderObj.getPrice()))
								{
									removalList.add(orderData);
								}
							}
							askOrderBook.removeAll(removalList);
													
							
							if (!(updateOrderObj.getQuantity().equals(0.0))) {
								askOrderBook.add(updateOrderObj);
								
							}
							Collections.sort(askOrderBook, askComparator);
							bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
							
						} else if ((updateOrderObj.getUpdateType().equals("trade"))) {
							for (OrderData orderData : askOrderBook) {
								if(orderData.getPrice().equals(updateOrderObj.getPrice()))
								{
									removalList.add(orderData);
								}
							}
							askOrderBook.removeAll(removalList);
							
							
							if (!(updateOrderObj.getQuantity().equals(0.0))) {
								askOrderBook.add(updateOrderObj);
								
							}
							Collections.sort(askOrderBook, askComparator);
							bestBidAskdata = getBestBidAsk(machineTime, timeStamp);
							
						}
					}

				}
				orderBookSnap.clear();
				orderBookSnap.add(bidOrderBook);
				orderBookSnap.add(askOrderBook);
				FileOutputStream fos = null;
		        ObjectOutputStream out = null;
		        try {
		            fos = new FileOutputStream("/home/bizruntime/state/geminiADL"+currency+".txt",false);
		            out = new ObjectOutputStream(fos);
		            out.writeObject(orderBookSnap);
		            out.flush();
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
		orderObj.setUpdateType(orderJson.getString("UpdateType"));
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
			Collections.sort(askOrderBook, askComparator);
			Collections.sort(bidOrderBook, bidComparator);
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
