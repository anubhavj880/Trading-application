package co.biz.therock;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import co.biz.therock.sink.TheRockEthBtcADLSink;
import co.biz.therock.sink.TheRockLtcBtcADLSink;
import co.biz.therock.sink.TheRockXrpBtcADLSink;
import co.biz.therock.util.BestBidAskCalculator;
import co.biz.therock.util.MidPointCalculator;


public class TheRockToADLMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(TheRockToADLMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XRPBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator XRPBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = TheRockToADLMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
    		if(kafkaPropStream==null){
    	            slf4jLogger.error("Sorry, unable to find " + "kafka.properties");
    		    return;
    		}
    		
    		kafkaProp.load(kafkaPropStream);
    		
    		
    	} catch (IOException ex) {
    		 slf4jLogger.error(ex.getMessage());
        } finally{
        	if(kafkaPropStream!=null){
        		try {
        			kafkaPropStream.close();
			} catch (IOException e) {
				slf4jLogger.error(e.getMessage());
			}
        	}
        	
        }

		
		
		/**
		 * Adding the TheRock-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("TheRock-ETHBTC-order", new SimpleStringSchema(), kafkaProp),
				"Kafka_TheRock_ETHBTC_Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new TheRockEthBtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);
		ethbtc_MidPointStream.addSink(new TheRockEthBtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new TheRockEthBtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		
		/**
		 * Adding the THEROCK-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("TheRock-ETHBTC-trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_THEROCK-ETHBTC-Trade_Source").setParallelism(2);
		ethbtc_TradeStream.addSink(new TheRockEthBtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");

		
		/**
		 * Adding the THEROCK-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("TheRock-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_THEROCK-ETHBTC-Ticker_Source").setParallelism(2);
		ethbtc_TickerStream.addSink(new TheRockEthBtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");

		
		
	
//		/**
//		 * Adding the TheRock-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("TheRock-LTCBTC-order", new SimpleStringSchema(), properties),
//				"Kafka_TheRock-LTCBTC-Order_Source").setParallelism(10);
//		ltcbtc_RawOrderStream.addSink(new TheRockLtcBtcADLSink<String>()).setParallelism(25).name("LTCBTC_RawOrder_ADLSink");
////		ltcbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("LTCBTC_RawOrder_ESSink");
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);
//		ltcbtc_BestBidAskOrderStream.addSink(new TheRockLtcBtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
////		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new TheRockLtcBtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
////		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		
//	
//		/**
//		 * Adding the THEROCK-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("TheRock-LTCBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_THEROCK-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new TheRockLtcBtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
////		ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		
//		/**
//		 * Adding the THEROCK-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("TheRock-LTCBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_THEROCK-LTCBTC-Ticker_Source").setParallelism(2);
//		ltcbtc_TickerStream.addSink(new TheRockLtcBtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
////		ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//		
//		
//		
//		
//		/**
//		 * Adding the TheRock-XRPBTC-Order source to the execution environment
//		 */
//		DataStream<String> xrpbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("TheRock-BTCXRP-order", new SimpleStringSchema(), properties),
//				"Kafka_TheRock-XRPBTC-Order_Source").setParallelism(10);
//
//		xrpbtc_RawOrderStream.addSink(new TheRockXrpBtcADLSink<String>()).setParallelism(25).name("XRPBTC_RawOrder_ADLSink");
////		xrpbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XRPBTC_RawOrder_ESSink");
//		DataStream<String>  XRPBTC_BestBidAskOrderStream = XRPBTCBIDASKCAL.calcBestBidAskStream(xrpbtc_RawOrderStream);
//		XRPBTC_BestBidAskOrderStream.addSink(new TheRockXrpBtcADLSink<String>()).setParallelism(15).name("XRPBTC_BestBidAsk_ADLSink");
////		XRPBTC_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XRPBTC_BestBidAsk_ESSink");
//		DataStream<String>  XRPBTC_MidPointStream = XRPBTCMIDCAL.calculateMidPoint(XRPBTC_BestBidAskOrderStream);
//		XRPBTC_MidPointStream.addSink(new TheRockXrpBtcADLSink<String>()).setParallelism(15).name("XRPBTC_Midpoint_ADLSink");
////		XRPBTC_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XRPBTC_Midpoint_ESSink");
//		/**
//		 * Adding the BitFinex-XRPBTC-Trade source to the execution environment
//		 */
//		DataStream<String> XRPBTC_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("TheRock-BTCXRP-Trade", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-XRPBTC-Trade_Source").setParallelism(2);
//		XRPBTC_TradeStream.addSink(new TheRockXrpBtcADLSink<String>()).setParallelism(3).name("XRPBTC_Trade_ADLSink");
////		XRPBTC_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XRPBTC_Trade_ESSink");
//		
//		/**
//		 * Adding the BitFinex-XRPBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> XRPBTC_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("TheRock-BTCXRP-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-XRPBTC-Ticker_Source").setParallelism(2);
//		XRPBTC_TickerStream.addSink(new TheRockXrpBtcADLSink<String>()).setParallelism(3).name("XRPBTC_Ticker_ADLSink");
////		XRPBTC_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XRPBTC_Ticker_ESSink");
//	
		env.execute("TheRock_Streaming_ADL_App");

	}

}
