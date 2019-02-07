package co.biz.orderpressurepriceindicator;

import java.util.Collections;
import static java.util.Comparator.*;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import static java.util.Comparator.naturalOrder;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dhinesh Raja
 *
 */
public class OrderPressurePriceIndicator {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(OrderPressurePriceIndicator.class);

	private static Double spread = 0.0;
	private static Double midpoint = 0.0;
	private static Double bestBid = 0.0;
	private static Double buyVolumeNearAsk = 0.0;
	private static Double sellVolumeNearBuy = 0.0;
	private static List<Order> bidOrderBook = new CopyOnWriteArrayList<Order>();
	private static List<Order> askOrderBook = new CopyOnWriteArrayList<Order>();

	private static Producer<String, String> producer;
	private final Properties properties = new Properties();

	public OrderPressurePriceIndicator() {
		properties.put("bootstrap.servers", "192.168.1.72:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("request.required.acks", "1");
		producer = new KafkaProducer<>(properties);
	}

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.1.75:9092");
		properties.setProperty("group.id", "BitfinexOrder");

		DataStream<String> orderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Order", new SimpleStringSchema(), properties),
				"Kafka_Source");

		DataStream<String> tickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Ticker", new SimpleStringSchema(), properties),
				"Kafka_Source");

		DataStream<Double> spreadMidpointStream = new OrderPressurePriceIndicator()
				.calculateSpreadAndMidpoint(tickerStream);

		SplitStream<String> bidAskSplitStream = orderStream.split(new OutputSelector<String>() {
			@Override
			public Iterable<String> select(String orderData) {
				JSONObject json = new JSONObject(orderData);
				List<String> output = new ArrayList<String>();
				if (json.get("OrderSide").toString().toLowerCase().equals("bid")) {
					output.add("bidStream");
				} else {
					output.add("askStream");
				}
				return output;
			}
		});

		DataStream<List<Order>> bidOrderBookStream = new OrderPressurePriceIndicator()
				.calcOrderPressureIndicatorWithBidStream(bidAskSplitStream.select("bidStream"));

		DataStream<List<Order>> askOrderBookStream = new OrderPressurePriceIndicator()
				.calcOrderPressureIndicatorWithAskStream(bidAskSplitStream.select("askStream"));
		/* new OrderPressurePriceIndicator().display(askOrderBookStream); */

		try {
			env.execute("App");
		} catch (Exception e) {
			slf4jLogger.info(e.getMessage());
			slf4jLogger.info("" + e.getCause());
		}
	}

	private void display(DataStream<List<Order>> askOrderBookStream) {
		DataStream<String> stream = askOrderBookStream.map(new MapFunction<List<Order>, String>() {

			@Override
			public String map(List<Order> orderData) throws Exception {

				for (Order order : orderData) {
					slf4jLogger.info(" AskOrders: " + order);
				}
				slf4jLogger.info(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
				return "";
			}

		});

	}

	private DataStream<List<Order>> calcOrderPressureIndicatorWithAskStream(DataStream<String> askOrderStream) {
		DataStream<List<Order>> askOrderBookStream = askOrderStream.map(new MapFunction<String, List<Order>>() {
			@Override
			public List<Order> map(String orderData) throws Exception {
				JSONObject json = new JSONObject(orderData);
				List<Order> removalList = new ArrayList<>();
				Order orderObj = new Order();
				orderObj.setExchangeName(json.getString("ExchangeName"));
				orderObj.setCurrencyPair(json.getString("CurrencyPair"));
				orderObj.setOrderSide(json.getString("OrderSide"));
				orderObj.setOrderId(json.getLong("OrderId"));
				orderObj.setTime(json.getString("MachineTime"));
				orderObj.setPrice(json.getDouble("Price"));
				orderObj.setQuantity(json.getDouble("Quantity"));

				/* slf4jLogger.info(" AskOrderSize: " + askOrderBook.size()); */

				if (askOrderBook.size() > 0) {
					for (Order order : askOrderBook) {

						/*
						 * slf4jLogger.info(
						 * "Going to compare this order from list: " + order +
						 * "  with   " + orderObj);
						 */

						if ((orderObj.getPrice().equals(0.0)) && (order.getOrderId().equals(orderObj.getOrderId()))) {

							/*
							 * slf4jLogger.
							 * info(" Order Removed from AskOrders: " + order);
							 */

							if (order.getPrice() <= (bestBid + 0.0035)) {
								buyVolumeNearAsk = order.getQuantity();
							}
							removalList.add(order);
							askOrderBook.removeAll(removalList);
							if (askOrderBook.size() > 1) {
								Comparator<Order> comparator = comparing(Order::getPrice, naturalOrder());
								Collections.sort(askOrderBook, comparator);
							}
						}
					}
					if (orderObj.getPrice() != 0.0) {
						if (orderObj.getPrice() <= (bestBid + 0.0035)) {
							sellVolumeNearBuy = orderObj.getQuantity();
						}
						askOrderBook.add(orderObj);
						Comparator<Order> comparator = comparing(Order::getPrice, naturalOrder());
						Collections.sort(askOrderBook, comparator);
					}
				} else {
					if (orderObj.getPrice() != 0.0) {
						if (orderObj.getPrice() <= (bestBid + 0.0035)) {
							sellVolumeNearBuy = orderObj.getQuantity();
						}
						askOrderBook.add(orderObj);
					}
				}
				if ((buyVolumeNearAsk > 0.0) || (sellVolumeNearBuy > 0.0)) {
					Double totalOrderPressureNearPrice = buyVolumeNearAsk + sellVolumeNearBuy;
					Double buyOrderPercentPressure = buyVolumeNearAsk / totalOrderPressureNearPrice;
					Double sellOrderPercentPressure = 1 - buyOrderPercentPressure;
					Double orderPressurePrice = bestBid + (spread * buyOrderPercentPressure);

					JSONObject jsonobj = new JSONObject();
					jsonobj.put("Spread", spread);
					jsonobj.put("BuyVolumeNearAsk", buyVolumeNearAsk);
					jsonobj.put("SellVolumeNearBuy", sellVolumeNearBuy);
					jsonobj.put("TotalOrderPressureNearPrice", totalOrderPressureNearPrice);
					jsonobj.put("BuyOrderPercentPressure", buyOrderPercentPressure);
					jsonobj.put("SellOrderPercentPressure", sellOrderPercentPressure);
					jsonobj.put("OrderPressurePrice", orderPressurePrice);

					String orderPressurePriceIndicator = jsonobj.toString();
					ProducerRecord<String, String> orderPressurePriceIndicatorData = new ProducerRecord<>(
							"BitfinexOrderPressurePriceIndicator", orderPressurePriceIndicator);
					producer.send(orderPressurePriceIndicatorData);
					slf4jLogger.info(orderPressurePriceIndicator);
					slf4jLogger.info("BitfinexOrderPressurePriceIndicator Sent Successfully from askOrderStream");
				}
				return askOrderBook;
			}
		});
		return askOrderBookStream;
	}

	private DataStream<List<Order>> calcOrderPressureIndicatorWithBidStream(DataStream<String> bidOrderStream) {
		DataStream<List<Order>> bidOrderBookStream = bidOrderStream.map(new MapFunction<String, List<Order>>() {
			@Override
			public List<Order> map(String orderData) throws Exception {
				JSONObject json = new JSONObject(orderData);
				List<Order> removalList = new ArrayList<>();
				Order orderObj = new Order();
				orderObj.setExchangeName(json.getString("ExchangeName"));
				orderObj.setCurrencyPair(json.getString("CurrencyPair"));
				orderObj.setOrderSide(json.getString("OrderSide"));
				orderObj.setOrderId(json.getLong("OrderId"));
				orderObj.setTime(json.getString("MachineTime"));
				orderObj.setPrice(json.getDouble("Price"));
				orderObj.setQuantity(json.getDouble("Quantity"));

				/*
				 * slf4jLogger. info("***************** BidOrderBookSize: " +
				 * bidOrderBook.size() + "  ***************");
				 */

				if (bidOrderBook.size() > 0) {
					for (Order order : bidOrderBook) {

						/*
						 * slf4jLogger.
						 * info("################# Going to compare this order from list: "
						 * + order + "  with   " + orderObj +
						 * " ####################");
						 */
						if ((orderObj.getPrice().equals(0.0)) && (order.getOrderId().equals(orderObj.getOrderId()))) {

							/*
							 * slf4jLogger.info(
							 * "$$$$$$$$$$$$$$$ Order Removed from bidOrderBook: "
							 * + order + " $$$$$$$$$$$$$$$");
							 */
							if (order.getPrice() >= (bestBid - 0.0035)) {
								sellVolumeNearBuy = order.getQuantity();
							}

							removalList.add(order);
							bidOrderBook.removeAll(removalList);
							if (bidOrderBook.size() > 1) {
								Comparator<Order> comparator = comparing(Order::getPrice, reverseOrder());
								Collections.sort(bidOrderBook, comparator);
							}
						}
					}
					if (orderObj.getPrice() != 0.0) {
						if (orderObj.getPrice() >= (bestBid - 0.0035)) {
							buyVolumeNearAsk = orderObj.getQuantity();
						}

						bidOrderBook.add(orderObj);
						Comparator<Order> comparator = comparing(Order::getPrice, reverseOrder());
						Collections.sort(bidOrderBook, comparator);

					}
				} else {
					if (orderObj.getPrice() != 0.0) {
						if (orderObj.getPrice() >= (bestBid - 0.0035)) {
							buyVolumeNearAsk = orderObj.getQuantity();
						}
						bidOrderBook.add(orderObj);
					}
				}
				if ((buyVolumeNearAsk > 0.0) || (sellVolumeNearBuy > 0.0)) {
					Double totalOrderPressureNearPrice = buyVolumeNearAsk + sellVolumeNearBuy;
					Double buyOrderPercentPressure = buyVolumeNearAsk / totalOrderPressureNearPrice;
					Double sellOrderPercentPressure = 1 - buyOrderPercentPressure;
					Double orderPressurePrice = bestBid + (spread * buyOrderPercentPressure);

					JSONObject jsonobj = new JSONObject();
					jsonobj.put("Spread", spread);
					jsonobj.put("BuyVolumeNearAsk", buyVolumeNearAsk);
					jsonobj.put("SellVolumeNearBuy", sellVolumeNearBuy);
					jsonobj.put("TotalOrderPressureNearPrice", totalOrderPressureNearPrice);
					jsonobj.put("BuyOrderPercentPressure", buyOrderPercentPressure);
					jsonobj.put("SellOrderPercentPressure", sellOrderPercentPressure);
					jsonobj.put("OrderPressurePrice", orderPressurePrice);

					String orderPressurePriceIndicator = jsonobj.toString();
					ProducerRecord<String, String> orderPressurePriceIndicatorData = new ProducerRecord<>(
							"BitfinexOrderPressurePriceIndicator", orderPressurePriceIndicator);
					producer.send(orderPressurePriceIndicatorData);
					slf4jLogger.info(orderPressurePriceIndicator);
					slf4jLogger.info("BitfinexOrderPressurePriceIndicator Sent Successfully from BidOrderStream");
				}
				return bidOrderBook;
			}
		});
		return bidOrderBookStream;
	}

	private DataStream<Double> calculateSpreadAndMidpoint(DataStream<String> tickerStream) {
		DataStream<Double> spreadMidStream = tickerStream.map(new MapFunction<String, Double>() {
			@Override
			public Double map(String tickerData) throws Exception {
				JSONObject json = new JSONObject(tickerData);
				bestBid = (Double) json.get("Bid");
				Double bestAsk = (Double) json.get("Ask");
				spread = bestAsk - bestBid;
				midpoint = (bestAsk + bestBid) / 2;
				slf4jLogger.info("spread , " + spread + " midpoint , " + midpoint);
				return midpoint;
			}
		});
		return spreadMidStream;
	}
}
