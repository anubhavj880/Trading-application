package co.biz.volumepressureindicator;

import java.util.Properties;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
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

public class VolumePressureIndicator {

	private static Double spread = 0.0;
	private static Double midpoint = 0.0;
	private static Double buySideVolumePressure = 0.0;
	private static Double sellSideVolumePressure = 0.0;
	private static Double bid = 0.0;
	private static Producer<String, String> producer;
	private final Properties properties = new Properties();

	public VolumePressureIndicator() {
		properties.put("bootstrap.servers", "192.168.1.72:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("request.required.acks", "1");
		producer = new KafkaProducer<>(properties);
	}

	private final static Logger slf4jLogger = LoggerFactory.getLogger(VolumePressureIndicator.class);

	public static void main(String[] args) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "192.168.1.75:9092");
		properties.setProperty("group.id", "BitfinexVolume");

		DataStream<String> tradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Trade", new SimpleStringSchema(), properties),
				"Kafka_Source");
		DataStream<String> tickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Ticker", new SimpleStringSchema(), properties),
				"Kafka_Source");

		DataStream<Double> spreadMidpointStream = new VolumePressureIndicator()
				.calculateSpreadAndMidpoint(tickerStream);

		DataStream<String> buyPressureTradeStream = new VolumePressureIndicator().calcBuyPressureTrade(tradeStream);
		DataStream<String> sellPressureTradeStream = new VolumePressureIndicator().calcSellPressureTrade(tradeStream);
		DataStream<Double> buySideVolumeWMAStream = new VolumePressureIndicator()
				.calcBuySideVolumeWMA(buyPressureTradeStream);
		DataStream<Double> sellSideVolumeWMAStream = new VolumePressureIndicator()
				.calcSellSideVolumeWMA(sellPressureTradeStream);

		try {
			env.execute("App");
		} catch (Exception e) {
			slf4jLogger.info(e.getMessage());
		}
	}

	private DataStream<String> calcBuyPressureTrade(DataStream<String> tradeStream) {
		DataStream<String> buyPressureTradeStream = tradeStream.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				JSONObject json = new JSONObject(value);
				Double price = (Double) json.get("Price");
				/*
				 * slf4jLogger.info("Midpoint: " + midpoint + ", TradePrice: " +
				 * price);
				 */
				return price >= midpoint;
			}
		});
		return buyPressureTradeStream;
	}

	private DataStream<String> calcSellPressureTrade(DataStream<String> tradeStream) {
		DataStream<String> sellPressureTradeStream = tradeStream.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				JSONObject json = new JSONObject(value);
				Double price = (Double) json.get("Price");
				return price < midpoint;
			}
		});
		return sellPressureTradeStream;
	}

	private DataStream<Double> calculateSpreadAndMidpoint(DataStream<String> tickerStream) {
		DataStream<Double> spreadMidStream = tickerStream.map(new MapFunction<String, Double>() {
			@Override
			public Double map(String tickerData) throws Exception {
				JSONObject json = new JSONObject(tickerData);
				bid = (Double) json.get("Bid");
				Double bestAsk = (Double) json.get("Ask");
				spread = bestAsk - bid;
				midpoint = (bestAsk + bid) / 2;
				slf4jLogger.info("spread , " + spread + " midpoint , " + midpoint);
				return midpoint;
			}
		});
		return spreadMidStream;
	}

	private DataStream<Double> calcBuySideVolumeWMA(DataStream<String> buyPressureTradeStream) {
		Integer windowSize = 3;
		Integer windowslide = 1;
		DataStream<Double> buySideVolumeWMAStream = buyPressureTradeStream.countWindowAll(windowSize, windowslide)
				.trigger(CountTrigger.of(windowSize)).apply(new AllWindowFunction<String, Double, GlobalWindow>() {
					@Override
					public void apply(GlobalWindow window, Iterable<String> values, Collector<Double> out)
							throws Exception {
						Double buySideVolumeWMA = 0.0;
						Integer weight = windowSize;
						Integer numerator = 1;

						for (String tradeString : values) {
							slf4jLogger.info("BuySideTradeStream: " + tradeString);
							JSONObject json = new JSONObject(tradeString);
							Double tradeVolume = (Double) json.get("Volume");
							buySideVolumeWMA += ((tradeVolume * numerator) / weight);
							slf4jLogger.info("tradeVolume " + tradeVolume + " , " + "numerator , " + numerator
									+ " weight , " + weight + " buySideVolumeWMA " + buySideVolumeWMA);
							numerator++;
						}
						numerator = 1;
						out.collect(buySideVolumeWMA / 2);
						buySideVolumePressure = buySideVolumeWMA / 2;
						/*
						 * slf4jLogger.info("buySideVolumePressure :" +
						 * buySideVolumePressure);
						 */
						Double totalVolumePressure = buySideVolumePressure + sellSideVolumePressure;
						Double buyPercentPressure = buySideVolumePressure / totalVolumePressure;
						Double sellPercentPressure = 1 - buyPercentPressure;
						Double tradePressurePrice = bid + (spread * buyPercentPressure);
						JSONObject jsonobj = new JSONObject();
						jsonobj.put("Spread", spread);
						jsonobj.put("Midpoint", midpoint);
						jsonobj.put("BuySideVolumePressure", buySideVolumePressure);
						jsonobj.put("SellSideVolumePressure", sellSideVolumePressure);
						jsonobj.put("TotalVolumePressure", totalVolumePressure);
						jsonobj.put("BuyPercentPressure", buyPercentPressure);
						jsonobj.put("SellPercentPressure", sellPercentPressure);
						jsonobj.put("TradePressurePrice", tradePressurePrice);

						String volumePressureIndicator = jsonobj.toString();
						ProducerRecord<String, String> volumePressureIndicatorData = new ProducerRecord<>(
								"BitfinexvolumePressureIndicator", volumePressureIndicator);
						producer.send(volumePressureIndicatorData);
						slf4jLogger.info("BitfinexvolumePressureIndicator Message Sent Successfully");
						slf4jLogger.info("Spread : " + spread + " , midpoint : " + midpoint
								+ " , buySideVolumePressure : " + buySideVolumePressure + " , sellSideVolumePressure : "
								+ sellSideVolumePressure + " , totalVolumePressure : " + totalVolumePressure
								+ " , buyPercentPressure : " + buyPercentPressure + " , sellPercentPressure : "
								+ sellPercentPressure + " , tradePressurePrice : " + tradePressurePrice);
					}
				});
		buySideVolumeWMAStream.print().setParallelism(5);
		return buySideVolumeWMAStream;
	}

	private DataStream<Double> calcSellSideVolumeWMA(DataStream<String> sellPressureTradeStream) {
		Integer windowSize = 3;
		Integer windowslide = 1;
		DataStream<Double> sellSideVolumeWMAStream = sellPressureTradeStream.countWindowAll(windowSize, windowslide)
				.trigger(CountTrigger.of(3)).apply(new AllWindowFunction<String, Double, GlobalWindow>() {
					@Override
					public void apply(GlobalWindow window, Iterable<String> values, Collector<Double> out)
							throws Exception {
						Double sellSideVolumeWMA = 0.0;
						Integer weight = windowSize;
						Integer numerator = 1;

						for (String tradeString : values) {
							slf4jLogger.info("SellSideTradeStream: " + tradeString);
							JSONObject json = new JSONObject(tradeString);
							Double tradeVolume = (Double) json.get("Volume");
							sellSideVolumeWMA += ((tradeVolume * numerator) / weight);
							slf4jLogger.info("tradeVolume " + tradeVolume + " , " + "numerator , " + numerator
									+ " weight , " + weight + " sellSideVolumeWMA " + sellSideVolumeWMA);
							numerator++;
						}
						numerator = 1;
						out.collect(sellSideVolumeWMA / 2);
						sellSideVolumePressure = sellSideVolumeWMA / 2;
						/*
						 * slf4jLogger.info("sellSideVolumePressure :" +
						 * sellSideVolumePressure);
						 */
						Double totalVolumePressure = buySideVolumePressure + sellSideVolumePressure;
						Double buyPercentPressure = buySideVolumePressure / totalVolumePressure;
						Double sellPercentPressure = 1 - buyPercentPressure;
						Double tradePressurePrice = bid + (spread * buyPercentPressure);
						JSONObject jsonobj = new JSONObject();
						jsonobj.put("Spread", spread);
						jsonobj.put("Midpoint", midpoint);
						jsonobj.put("BuySideVolumePressure", buySideVolumePressure);
						jsonobj.put("SellSideVolumePressure", sellSideVolumePressure);
						jsonobj.put("TotalVolumePressure", totalVolumePressure);
						jsonobj.put("BuyPercentPressure", buyPercentPressure);
						jsonobj.put("SellPercentPressure", sellPercentPressure);
						jsonobj.put("TradePressurePrice", tradePressurePrice);

						String volumePressureIndicator = jsonobj.toString();
						ProducerRecord<String, String> volumePressureIndicatorData = new ProducerRecord<>(
								"BitfinexvolumePressureIndicator", volumePressureIndicator);
						producer.send(volumePressureIndicatorData);
						slf4jLogger.info("BitfinexvolumePressureIndicator Message Sent Successfully");
						slf4jLogger.info("Spread : " + spread + " , midpoint : " + midpoint
								+ " , buySideVolumePressure : " + buySideVolumePressure + " , sellSideVolumePressure : "
								+ sellSideVolumePressure + " , totalVolumePressure : " + totalVolumePressure
								+ " , buyPercentPressure : " + buyPercentPressure + " , sellPercentPressure : "
								+ sellPercentPressure + " , tradePressurePrice : " + tradePressurePrice);
					}
				});
		sellSideVolumeWMAStream.print().setParallelism(5);
		return sellSideVolumeWMAStream;
	}
}
