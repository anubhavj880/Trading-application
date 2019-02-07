package co.biz.livecoin;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import co.biz.livecoin.sink.LiveCoinDshbtcADLSink;
import co.biz.livecoin.sink.LiveCoinEthbtcADLSink;
import co.biz.livecoin.sink.LiveCoinLtcbtcADLSink;
import co.biz.livecoin.sink.LiveCoinXmrbtcADLSink;
import co.biz.livecoin.util.BestBidAskCalculator;
import co.biz.livecoin.util.MidPointCalculator;




public class LiveCoinToADLMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(LiveCoinToADLMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator DSHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator DSHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XMRBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator XMRBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = LiveCoinToADLMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the LiveCoin-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("LiveCoin-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_LiveCoin_ETHBTC_Order_Source").setParallelism(5);

		ethbtc_RawOrderStream.addSink(new LiveCoinEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);
		ethbtc_MidPointStream.addSink(new LiveCoinEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new LiveCoinEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		/**
		 * Adding the LiveCoin-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("LiveCoin-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_LiveCoin_ETHBTC_Trade_Source").setParallelism(2);
		ethbtc_TradeStream.addSink(new LiveCoinEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");
	

		/**
		 * Adding the LiveCoin-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("LiveCoin-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_LiveCoin_ETHBTC_Ticker_Source").setParallelism(2);
		ethbtc_TickerStream.addSink(new LiveCoinEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADlSink");
		
//
//		/**
//		 * Adding the LiveCoin-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-LTCBTC-Order_Source").setParallelism(10);
//		ltcbtc_RawOrderStream.addSink(new LiveCoinLtcbtcADLSink<String>()).setParallelism(25).name("LTCBTC_RawOrder_ADLSink");
////		ltcbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("LTCBTC_RawOrder_ESSink");
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);
//		ltcbtc_BestBidAskOrderStream.addSink(new LiveCoinLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
////		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new LiveCoinLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
////		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		/**
//		 * Adding the LiveCoin-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-LTCBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new LiveCoinLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
//		//ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		/**
//		 * Adding the LiveCoin-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-LTCBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-LTCBTC-Ticker_Source").setParallelism(2);
//		ltcbtc_TickerStream.addSink(new LiveCoinLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
//		//ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//
//		
//		/**
//		 * Adding the LiveCoin-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-DSHBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-DSHBTC-Order_Source").setParallelism(10);
//		dshbtc_RawOrderStream.addSink(new LiveCoinDshbtcADLSink<String>()).setParallelism(25).name("DSHBTC_RawOrder_ADLSink");
////		dshbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("DSHBTC_RawOrder_ESSink");
//		DataStream<String>  dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream);
//		dshbtc_BestBidAskOrderStream.addSink(new LiveCoinDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_BestBidAsk_ADLSink");
////		dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		DataStream<String>  dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		dshbtc_MidPointStream.addSink(new LiveCoinDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_Midpoint_ADLSink");
////		dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		/**
//		 * Adding the LiveCoin-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-DSHBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-DSHBTC-Trade_Source").setParallelism(2);
//		dshbtc_TradeStream.addSink(new LiveCoinDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Trade_ADLSink");
//		//dshbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("DSHBTC_Trade_ESSink");
//		
//		/**
//		 * Adding the LiveCoin-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-DSHBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-DSHBTC-Ticker_Source").setParallelism(2);
//		dshbtc_TickerStream.addSink(new LiveCoinDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Ticker_ADLSink");
//		//dshbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("DSHBTC_Ticker_ESSink");
//		
//		
//		/**
//		 * Adding the LiveCoin-XMRBTC-Order source to the execution environment
//		 */
//		DataStream<String> xmrbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-XMRBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-XMRBTC-Order_Source").setParallelism(10);
//		xmrbtc_RawOrderStream.addSink(new LiveCoinXmrbtcADLSink<String>()).setParallelism(25).name("XMRBTC_RawOrder_ADLSink");
////		xmrbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XMRBTC_RawOrder_ESSink");
//		DataStream<String>  xmrbtc_BestBidAskOrderStream = XMRBTCBIDASKCAL.calcBestBidAskStream(xmrbtc_RawOrderStream);
//		xmrbtc_BestBidAskOrderStream.addSink(new LiveCoinXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_BestBidAsk_ADLSink");
////		xmrbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XMRBTC_BestBidAsk_ESSink");
//		DataStream<String>  xmrbtc_MidPointStream = XMRBTCMIDCAL.calculateMidPoint(xmrbtc_BestBidAskOrderStream);
//		xmrbtc_MidPointStream.addSink(new LiveCoinXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_Midpoint_ADLSink");
////		xmrbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XMRBTC_Midpoint_ESSink");
//		/**
//		 * Adding the LiveCoin-XMRBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-XMRBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-XMRBTC-Trade_Source").setParallelism(2);
//		xmrbtc_TradeStream.addSink(new LiveCoinXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Trade_ADLSink");
//		//xmrbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XMRBTC_Trade_ESSink");
//		/**
//		 * Adding the LiveCoin-XMRBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("LiveCoin-XMRBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_LiveCoin-XMRBTC-Ticker_Source").setParallelism(2);
//		xmrbtc_TickerStream.addSink(new LiveCoinXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Ticker_ADLSink");
//		//xmrbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XMRBTC_Ticker_ESSink");

		

		env.execute("LiveCoin_Streaming_App");

	}

}
