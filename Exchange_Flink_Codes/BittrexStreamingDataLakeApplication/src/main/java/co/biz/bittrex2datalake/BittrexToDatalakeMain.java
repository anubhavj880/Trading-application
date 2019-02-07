package co.biz.bittrex2datalake;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import co.biz.bittrex2datalake.sink.BittrexEthbtcADLSink;
import co.biz.bittrex2datalake.util.BestBidAskCalculator;
import co.biz.bittrex2datalake.util.MidPointCalculator;


public class BittrexToDatalakeMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BittrexToDatalakeMain.class);
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
		env.enableCheckpointing(3000);

		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = BittrexToDatalakeMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the Bittrex-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Bittrex-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_Bittrex-ETHBTC-Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new BittrexEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);
		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);
		ethbtc_MidPointStream.addSink(new BittrexEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new BittrexEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		
		/**
		 * Adding the Bittrex-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Bittrex-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_Bittrex-ETHBTC-Trade_Source").setParallelism(1);
		ethbtc_TradeStream.addSink(new BittrexEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");

		/**
		 * Adding the Bittrex-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Bittrex-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_Bittrex-ETHBTC-Ticker_Source").setParallelism(1);
		ethbtc_TickerStream.addSink(new BittrexEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");
		
	

//		/**
//		 * Adding the Bittrex-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-LTCBTC-Order_Source").setParallelism(10);
//		ltcbtc_RawOrderStream.addSink(new BittrexLtcbtcADLSink<String>()).name("LTCBTC_RawOrder_ADLSink").setParallelism(25);

//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);

//		ltcbtc_BestBidAskOrderStream.addSink(new BittrexLtcbtcADLSink<String>()).name("LTCBTC_BestBidAskOrder_ADLSink").setParallelism(15);
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);

//		ltcbtc_MidPointStream.addSink(new BittrexLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
//		
//		/**
//		 * Adding the Bittrex-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-LTCBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new BittrexLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
//		//ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		/**
//		 * Adding the Bittrex-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-LTCBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-LTCBTC-Ticker_Source").setParallelism(8);
//		ltcbtc_TickerStream.addSink(new BittrexLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
//		//ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//
//		
//		/**
//		 * Adding the Bittrex-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-DSHBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-DSHBTC-Order_Source").setParallelism(10);
//		dshbtc_RawOrderStream.addSink(new BittrexDshbtcADLSink<String>()).name("DSHBTC_RawOrder_ADLSink").setParallelism(25);
////		dshbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("DSHBTC_RawOrder_ESSink");
//		DataStream<String>  dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream);
////		dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		dshbtc_BestBidAskOrderStream.addSink(new BittrexDshbtcADLSink<String>()).name("DSHBTC_BestBidAskOrder_ADLSink").setParallelism(15);
//		DataStream<String>  dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
////		dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		dshbtc_MidPointStream.addSink(new BittrexDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_Midpoint_ADLSink");
//		/**
//		 * Adding the Bittrex-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-DSHBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-DSHBTC-Trade_Source").setParallelism(2);
//		dshbtc_TradeStream.addSink(new BittrexDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Trade_ADLSink");
//		//dshbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("DSHBTC_Trade_ESSink");
//		/**
//		 * Adding the Bittrex-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-DSHBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-DSHBTC-Ticker_Source").setParallelism(2);
//		dshbtc_TickerStream.addSink(new BittrexDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Ticker_ADLSink");
//		//dshbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("DSHBTC_Ticker_ESSink");
//
//		
//		/**
//		 * Adding the Bittrex-XMRBTC-Order source to the execution environment
//		 */
//		DataStream<String> xmrbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-XMRBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-XMRBTC-Order_Source").setParallelism(10);
//		xmrbtc_RawOrderStream.addSink(new BittrexXmrbtcADLSink<String>()).name("XMRBTC_RawOrder_ADLSink").setParallelism(25);
////		xmrbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XMRBTC_RawOrder_ESSink");
//		DataStream<String>  xmrbtc_BestBidAskOrderStream = XMRBTCBIDASKCAL.calcBestBidAskStream(xmrbtc_RawOrderStream);
//		xmrbtc_BestBidAskOrderStream.addSink(new BittrexXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_BestBidAsk_ADLSink");
////		xmrbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XMRBTC_BestBidAsk_ESSink");
//		DataStream<String>  xmrbtc_MidPointStream = XMRBTCMIDCAL.calculateMidPoint(xmrbtc_BestBidAskOrderStream);
//		xmrbtc_MidPointStream.addSink(new BittrexXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_Midpoint_ADLSink");
////		xmrbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XMRBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Bittrex-XMRBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-XMRBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-XMRBTC-Trade_Source").setParallelism(2);
//		xmrbtc_TradeStream.addSink(new BittrexXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Trade_ADLSink");
//		//xmrbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XMRBTC_Trade_ESSink");
//		/**
//		 * Adding the Bittrex-XMRBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-XMRBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-XMRBTC-Ticker_Source").setParallelism(2);
//		xmrbtc_TickerStream.addSink(new BittrexXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Ticker_ADLSink");
//		//xmrbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XMRBTC_Ticker_ESSink");
//
//		
//
//		/**
//		 * Adding the Bittrex-XRPBTC-Order source to the execution environment
//		 */
//		DataStream<String> xrpbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-XRPBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-XRPBTC-Order_Source").setParallelism(10);
//		xrpbtc_RawOrderStream.addSink(new BittrexXrpbtcADLSink<String>()).setParallelism(15).name("XRPBTC_RawOrder_ADLSink");
////		xrpbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XRPBTC_RawOrder_ESSink");
//		DataStream<String>  xrpbtc_BestBidAskOrderStream = XRPBTCBIDASKCAL.calcBestBidAskStream(xrpbtc_RawOrderStream);
//		xrpbtc_BestBidAskOrderStream.addSink(new BittrexXrpbtcADLSink<String>()).setParallelism(15).name("XRPBTC_BestBidAsk_ADLSink");
////		xrpbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XRPBTC_BestBidAsk_ESSink");
//		DataStream<String>  xrpbtc_MidPointStream = XRPBTCMIDCAL.calculateMidPoint(xrpbtc_BestBidAskOrderStream);
//		xrpbtc_MidPointStream.addSink(new BittrexXrpbtcADLSink<String>()).setParallelism(15).name("XRPBTC_Midpoint_ADLSink");
////		xrpbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XRPBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Bittrex-XRPBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xrpbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-XRPBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-XRPBTC-Trade_Source").setParallelism(2);
//		xrpbtc_TradeStream.addSink(new BittrexXrpbtcADLSink<String>()).setParallelism(3).name("XRPBTC_Trade_ADLSink");
//		//xrpbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XRPBTC_Trade_ESSink");
//		/**
//		 * Adding the Bittrex-XRPBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xrpbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Bittrex-XRPBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Bittrex-XRPBTC-Ticker_Source").setParallelism(2);
//		xrpbtc_TickerStream.addSink(new BittrexXrpbtcADLSink<String>()).setParallelism(3).name("XRPBTC_Ticker_ADLSink");
//		//xrpbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XRPBTC_Ticker_ESSink");
		env.execute("Bitrex_Streaming_Datalake_App");

	}

}
