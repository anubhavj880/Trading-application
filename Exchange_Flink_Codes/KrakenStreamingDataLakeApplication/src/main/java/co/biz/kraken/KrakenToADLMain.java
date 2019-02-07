package co.biz.kraken;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.biz.kraken.sink.KrakenDshbtcADLSink;
import co.biz.kraken.sink.KrakenEthbtcADLSink;
import co.biz.kraken.sink.KrakenLtcbtcADLSink;
import co.biz.kraken.sink.KrakenXmrbtcADLSink;
import co.biz.kraken.sink.KrakenXrpbtcADLSink;
import co.biz.kraken.util.BestBidAskCalculator;
import co.biz.kraken.util.MidPointCalculator;

public class KrakenToADLMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(KrakenToADLMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator DSHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator DSHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XMRBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator XMRBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XRPBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator XRPBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(2000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = KrakenToADLMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the Kraken-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Kraken-XETHXXBT-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_Kraken_ETHBTC_Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new KrakenEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);

		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);

		ethbtc_MidPointStream.addSink(new KrakenEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new KrakenEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		
		/**
		 * Adding the Kraken-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Kraken-XETHXXBT-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_Kraken_ETHBTC_Trade_Source").setParallelism(2);
		ethbtc_TradeStream.addSink(new KrakenEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");


		/**
		 * Adding the Kraken-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Kraken-XETHXXBT-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_Kraken_ETHBTC_Ticker_Source").setParallelism(2);
		ethbtc_TickerStream.addSink(new KrakenEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");

		
//	
//
//		/**
//		 * Adding the Kraken-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XLTCXXBT-Order", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-LTCBTC-Order_Source").setParallelism(10);
//		ltcbtc_RawOrderStream.addSink(new KrakenLtcbtcADLSink<String>()).setParallelism(25).name("LTCBTC_RawOrder_ADLSink");
////		ltcbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("LTCBTC_RawOrder_ESSink");
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);
//		ltcbtc_BestBidAskOrderStream.addSink(new KrakenLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
////		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new KrakenLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
////		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		
//		/**
//		 * Adding the Kraken-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XLTCXXBT-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new KrakenLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
////		ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		/**
//		 * Adding the Kraken-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XLTCXXBT-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-LTCBTC-Ticker_Source").setParallelism(2);
//		ltcbtc_TickerStream.addSink(new KrakenLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
////		ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//
//		
//		/**
//		 * Adding the Kraken-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-DASHXBT-Order", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-DSHBTC-Order_Source").setParallelism(10);
//		dshbtc_RawOrderStream.addSink(new KrakenDshbtcADLSink<String>()).setParallelism(25).name("DSHBTC_RawOrder_ADLSink");
////		dshbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("DSHBTC_RawOrder_ESSink");
//		DataStream<String>  dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream);
//		dshbtc_BestBidAskOrderStream.addSink(new KrakenDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_BestBidAsk_ADLSink");
////		dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		DataStream<String>  dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		dshbtc_MidPointStream.addSink(new KrakenDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_Midpoint_ADLSink");
////		dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Kraken-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-DASHXBT-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-DSHBTC-Trade_Source").setParallelism(2);
//		dshbtc_TradeStream.addSink(new KrakenDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Trade_ADLSink");
////		dshbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("DSHBTC_Trade_ESSink");
//		/**
//		 * Adding the Kraken-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-DASHXBT-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-DSHBTC-Ticker_Source").setParallelism(2);
//		dshbtc_TickerStream.addSink(new KrakenDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Ticker_ADLSink");
////		dshbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("DSHBTC_Ticker_ESSink");
//
//		
//		/**
//		 * Adding the Kraken-XMRBTC-Order source to the execution environment
//		 */
//		DataStream<String> xmrbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XXMRXXBT-Order", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-XMRBTC-Order_Source").setParallelism(10);
//		xmrbtc_RawOrderStream.addSink(new KrakenXmrbtcADLSink<String>()).setParallelism(25).name("XMRBTC_RawOrder_ADLSink");
////		xmrbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XMRBTC_RawOrder_ESSink");
//		DataStream<String>  xmrbtc_BestBidAskOrderStream = XMRBTCBIDASKCAL.calcBestBidAskStream(xmrbtc_RawOrderStream);
//		xmrbtc_BestBidAskOrderStream.addSink(new KrakenXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_BestBidAsk_ADLSink");
////		xmrbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XMRBTC_BestBidAsk_ESSink");
//		DataStream<String>  xmrbtc_MidPointStream = XMRBTCMIDCAL.calculateMidPoint(xmrbtc_BestBidAskOrderStream);
//		xmrbtc_MidPointStream.addSink(new KrakenXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_Midpoint_ADLSink");
////		xmrbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XMRBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Kraken-XMRBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XXMRXXBT-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-XMRBTC-Trade_Source").setParallelism(2);
//		xmrbtc_TradeStream.addSink(new KrakenXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Trade_ADLSink");
////		xmrbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XMRBTC_Trade_ESSink");
//		/**
//		 * Adding the Kraken-XMRBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XXMRXXBT-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-XMRBTC-Ticker_Source").setParallelism(2);
//		xmrbtc_TickerStream.addSink(new KrakenXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Ticker_ADLSink");
////		xmrbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XMRBTC_Ticker_ESSink");
//
//		
//
//		/**
//		 * Adding the Kraken-XRPBTC-Order source to the execution environment
//		 */
//		DataStream<String> xrpbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XXRPXXBT-Order", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-XRPBTC-Order_Source").setParallelism(10);
//		xrpbtc_RawOrderStream.addSink(new KrakenXrpbtcADLSink<String>()).setParallelism(25).name("XRPBTC_RawOrder_ADLSink");
////		xrpbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XRPBTC_RawOrder_ESSink");
//		DataStream<String>  xrpbtc_BestBidAskOrderStream = XRPBTCBIDASKCAL.calcBestBidAskStream(xrpbtc_RawOrderStream);
//		xrpbtc_BestBidAskOrderStream.addSink(new KrakenXrpbtcADLSink<String>()).setParallelism(15).name("XRPBTC_BestBidAsk_ADLSink");
////		xrpbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XRPBTC_BestBidAsk_ESSink");
//		DataStream<String>  xrpbtc_MidPointStream = XRPBTCMIDCAL.calculateMidPoint(xrpbtc_BestBidAskOrderStream);
//		xrpbtc_MidPointStream.addSink(new KrakenXrpbtcADLSink<String>()).setParallelism(15).name("XRPBTC_Midpoint_ADLSink");
////		xrpbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XRPBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Kraken-XRPBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xrpbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XXRPXXBT-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-XRPBTC-Trade_Source").setParallelism(2);
//		xrpbtc_TradeStream.addSink(new KrakenXrpbtcADLSink<String>()).setParallelism(3).name("XRPBTC_Trade_ADLSink");
////		xrpbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XRPBTC_Trade_ESSink");
//		/**
//		 * Adding the Kraken-XRPBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xrpbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Kraken-XXRPXXBT-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Kraken-XRPBTC-Ticker_Source").setParallelism(2);
//		xrpbtc_TickerStream.addSink(new KrakenXrpbtcADLSink<String>()).setParallelism(3).name("XRPBTC_Ticker_ADLSink");
////		xrpbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XRPBTC_Ticker_ESSink");

		

		env.execute("Kraken_Streaming_ADL_App");

	}

}
