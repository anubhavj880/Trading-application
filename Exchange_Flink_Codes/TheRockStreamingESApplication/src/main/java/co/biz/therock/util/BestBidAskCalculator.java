package co.biz.therock.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
	private List<Order> bidOrderBook = new CopyOnWriteArrayList<Order>();
	private List<Order> askOrderBook = new CopyOnWriteArrayList<Order>();

	public DataStream<String> calcBestBidAskStream(DataStream<String> rawOrderStream) {

		DataStream<String> bestBidAskorderStream = rawOrderStream.map(new MapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String map(String rawOrderData) throws Exception {
				String bestBidAskdata = new JSONObject().toString();
				JSONObject rawJson = new JSONObject(rawOrderData);
				if (rawJson.has("Asks") && rawJson.has("Bids") && rawJson.getJSONArray("Asks").length() > 0 &&  rawJson.getJSONArray("Bids").length() > 0 ) {

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

	/*
	 * public DataStream<String> calcBestBidAskStream(DataStream<String>
	 * rawOrderStream, String ADLOrderlocation) {
	 * 
	 * try { bidAskOrderBook.createBidAkOrderBooks(ADLOrderlocation); } catch
	 * (IOException e) { logger.info(e.getMessage()); } catch (Exception e) {
	 * logger.info(e.getMessage()); } bidOrderBook =
	 * bidAskOrderBook.bidOrderBook; askOrderBook =
	 * bidAskOrderBook.askOrderBook;
	 * 
	 * DataStream<String> bestBidAskorderStream = rawOrderStream.map(new
	 * MapFunction<String, String>() {
	 * 
	 * private static final long serialVersionUID = 1L;
	 * 
	 * @Override public String map(String rawOrderData) throws Exception {
	 * String bestBidAskdata = new JSONObject().toString(); JSONObject rawJson =
	 * new JSONObject(rawOrderData); if (rawJson.has("Snapshot")) { bidOrderBook
	 * = new CopyOnWriteArrayList<Order>(); askOrderBook = new
	 * CopyOnWriteArrayList<Order>(); JSONArray nodes =
	 * rawJson.getJSONArray("Snapshot"); for (Object object : nodes) {
	 * JSONObject json = (JSONObject) object; Order snapshotOrderObj =
	 * getOrderObject(json); if (snapshotOrderObj.getOrderSide().equals("bids"))
	 * { bidOrderBook.add(snapshotOrderObj); bestBidAskdata =
	 * getBestBid(json.getString("MachineTime")).toString(); } else if
	 * (snapshotOrderObj.getOrderSide().equals("asks")) {
	 * askOrderBook.add(snapshotOrderObj); bestBidAskdata =
	 * getBestAsk(json.getString("MachineTime")).toString(); } }
	 * 
	 * } else if (rawJson.has("Updates")) { JSONObject json =
	 * rawJson.getJSONObject("Updates"); List<Order> removalList = new
	 * ArrayList<>(); Order updateOrderObj = getOrderObject(json); if
	 * (updateOrderObj.getOrderSide().equals("bids")) { for (Order order :
	 * bidOrderBook) { if ((updateOrderObj.getQuantity().equals(0.0)) &&
	 * (order.getPrice().equals(updateOrderObj.getPrice()))) {
	 * removalList.add(order); bidOrderBook.removeAll(removalList); if
	 * (bidOrderBook.size() > 1) { Comparator<Order> comparator =
	 * comparing(Order::getPrice, reverseOrder());
	 * Collections.sort(bidOrderBook, comparator); bestBidAskdata =
	 * getBestBid(updateOrderObj.getTime()).toString(); } } else if
	 * (!(updateOrderObj.getQuantity().equals(0.0)) &&
	 * (order.getPrice().equals(updateOrderObj.getPrice()))) {
	 * removalList.add(order); bidOrderBook.removeAll(removalList);
	 * bidOrderBook.add(updateOrderObj); Comparator<Order> comparator =
	 * comparing(Order::getPrice, reverseOrder());
	 * Collections.sort(bidOrderBook, comparator); bestBidAskdata =
	 * getBestBid(updateOrderObj.getTime()).toString(); } } if
	 * (!(updateOrderObj.getQuantity().equals(0.0)) && (removalList.size() ==
	 * 0)) {
	 * 
	 * bidOrderBook.add(updateOrderObj); if (bidOrderBook.size() > 1) {
	 * Comparator<Order> comparator = comparing(Order::getPrice,
	 * reverseOrder()); Collections.sort(bidOrderBook, comparator);
	 * bestBidAskdata = getBestBid(updateOrderObj.getTime()).toString(); } } }
	 * else if (updateOrderObj.getOrderSide().equals("asks")) { for (Order
	 * askorder : askOrderBook) { if ((updateOrderObj.getQuantity().equals(0.0))
	 * && (askorder.getPrice().equals(updateOrderObj.getPrice()))) {
	 * removalList.add(askorder); askOrderBook.removeAll(removalList); if
	 * (askOrderBook.size() > 1) { Comparator<Order> comparator =
	 * comparing(Order::getPrice, naturalOrder());
	 * Collections.sort(askOrderBook, comparator); bestBidAskdata =
	 * getBestAsk(updateOrderObj.getTime()).toString(); } } else if
	 * (!(updateOrderObj.getQuantity().equals(0.0)) &&
	 * (askorder.getPrice().equals(updateOrderObj.getPrice()))) {
	 * removalList.add(askorder); askOrderBook.removeAll(removalList);
	 * askOrderBook.add(updateOrderObj); if (askOrderBook.size() > 1) {
	 * Comparator<Order> comparator = comparing(Order::getPrice,
	 * naturalOrder()); Collections.sort(askOrderBook, comparator);
	 * bestBidAskdata = getBestAsk(updateOrderObj.getTime()).toString(); } } }
	 * if (!(updateOrderObj.getQuantity().equals(0.0)) && (removalList.size() ==
	 * 0)) { askOrderBook.add(updateOrderObj); if (askOrderBook.size() > 1) {
	 * Comparator<Order> comparator = comparing(Order::getPrice,
	 * naturalOrder()); Collections.sort(askOrderBook, comparator);
	 * bestBidAskdata = getBestAsk(updateOrderObj.getTime()).toString(); } } }
	 * 
	 * } return bestBidAskdata; } }); return bestBidAskorderStream; }
	 */

	private Order getOrderObject(JSONObject orderJson) {
		Order orderObj = new Order();
		orderObj.setExchangeName(orderJson.getString("ExchangeName"));
		orderObj.setCurrencyPair(orderJson.getString("CurrencyPair"));
		orderObj.setOrderSide(orderJson.getString("OrderSide"));
		orderObj.setTime(orderJson.getString("MachineTime"));
		orderObj.setPrice(orderJson.getDouble("Price"));
		orderObj.setQuantity(orderJson.getDouble("Quantity"));
		return orderObj;
	}

	private JSONObject getBestBid(String time) {
		JSONObject bidObj = new JSONObject();
		bidObj.put("ExchangeName", bidOrderBook.get(0).getExchangeName());
		bidObj.put("CurrencyPair", bidOrderBook.get(0).getCurrencyPair());
		bidObj.put("MachineTime", time);
		bidObj.put("OrderSide", bidOrderBook.get(0).getOrderSide());
		bidObj.put("Price", bidOrderBook.get(0).getPrice());
		bidObj.put("Quantity", bidOrderBook.get(0).getQuantity());
		return bidObj;
	}

	private JSONObject getBestAsk(String time) {
		JSONObject askObj = new JSONObject();
		askObj.put("ExchangeName", askOrderBook.get(0).getExchangeName());
		askObj.put("CurrencyPair", askOrderBook.get(0).getCurrencyPair());
		askObj.put("MachineTime", time);
		askObj.put("OrderSide", askOrderBook.get(0).getOrderSide());
		askObj.put("Price", askOrderBook.get(0).getPrice());
		askObj.put("Quantity", askOrderBook.get(0).getQuantity());
		return askObj;
	}

}
