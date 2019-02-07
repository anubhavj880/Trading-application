package co.biz.bitstamp;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.biz.bitstamp.sink.BitStampXrpbtcADLSink;
import co.biz.bitstamp.sink.ES_Sink;
import co.biz.bitstamp.util.BestBidAskCalculator;

import co.biz.bitstamp.util.MidPointCalculator;


public class BitStampExchangeApp {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitStampExchangeApp.class);
	private final static BestBidAskCalculator XRPBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator XRPBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//env.enableCheckpointing(5000);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "52.233.42.97:9092");
		properties.setProperty("group.id", "Bitstamp");
		properties.setProperty("request.timeout.ms", "305000");
		properties.setProperty("session.timeout.ms", "30000");
		properties.setProperty("enable.auto.commit", "true");
		properties.setProperty("reconnect.backoff.ms","50");
		

		/**
		 * Adding the BitStamp-XRPBTC-Order source to the execution environment
		 */
		DataStream<String> xrpbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitStamp-XRPBTC-order", new SimpleStringSchema(), properties),
				"Kafka_BitStamp-XRPBTC-Order_Source").setParallelism(6);

		xrpbtc_RawOrderStream.addSink(new BitStampXrpbtcADLSink<String>()).name("XRPBTC_RawOrder_ADLSink").setParallelism(15);
		xrpbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(10).name("XRPBTC_RawOrder_ESSink");
		DataStream<String>  xrpbtc_BestBidAskOrderStream = XRPBTCBIDASKCAL.calcBestBidAskStream(xrpbtc_RawOrderStream);
		xrpbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(10).name("XRPBTC_BestBidAsk_ESSink");
		DataStream<String>  xrpbtc_MidPointStream = XRPBTCMIDCAL.calculateMidPoint(xrpbtc_BestBidAskOrderStream);
		xrpbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(10).name("XRPBTC_Midpoint_ESSink");
		xrpbtc_MidPointStream.addSink(new BitStampXrpbtcADLSink<String>()).setParallelism(10).name("XRPBTC_Midpoint_ADLSink");
		xrpbtc_BestBidAskOrderStream.addSink(new BitStampXrpbtcADLSink<String>()).name("XRPBTC_BestBidAskOrder_ADLSink").setParallelism(10);

		
	
		
		/**
		 * Adding the BitStamp-XRPBTC-Trade source to the execution environment
		 */
		DataStream<String> xrpbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitStamp-XRPBTC-trade", new SimpleStringSchema(), properties),
				"Kafka_BitStamp-XRPBTC-Trade_Source").setParallelism(2);

		xrpbtc_TradeStream.addSink(new BitStampXrpbtcADLSink<String>()).name("XRPBTC_Trade_ADLSink").setParallelism(3);
		//xrpbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XRPBTC_Trade_ESSink");
		
		/**
		 * Adding the BitStamp-XRPBTC-Ticker source to the execution environment
		 */
		DataStream<String> xrpbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("BitStamp-XRPBTC-Ticker", new SimpleStringSchema(), properties),
				"Kafka_BitStamp-XRPBTC-Ticker_Source").setParallelism(2);

		xrpbtc_TickerStream.addSink(new BitStampXrpbtcADLSink<String>()).name("XRPBTC_Ticker_ADLSink").setParallelism(3);
		//xrpbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XRPBTC_Ticker_ESSink");
		env.execute("BitStampExchangeApplication");

	}

}
