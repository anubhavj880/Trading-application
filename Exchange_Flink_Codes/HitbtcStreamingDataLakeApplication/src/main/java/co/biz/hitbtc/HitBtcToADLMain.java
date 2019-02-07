package co.biz.hitbtc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.biz.hitbtc.sink.HitBtcDshbtcADLSink;
import co.biz.hitbtc.sink.HitBtcEthbtcADLSink;
import co.biz.hitbtc.sink.HitBtcLtcbtcADLSink;
import co.biz.hitbtc.sink.HitBtcXmrbtcADLSink;
import co.biz.hitbtc.util.BestBidAskCalculator;
import co.biz.hitbtc.util.MidPointCalculator;
import co.biz.hitbtc.util.OrderData;

public class HitBtcToADLMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(HitBtcToADLMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator DSHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator DSHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XMRBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator XMRBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {
		Tuple2<List<CopyOnWriteArrayList<OrderData>>,Long> ethbtcSnapData = null;
		FileInputStream ethbtcFileStream = null;
		ObjectInputStream ethbtcObjectStream = null;
		try {
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/hitbtcADLethbtc.txt");
			ethbtcObjectStream = new ObjectInputStream(ethbtcFileStream);
			Object ethObj = ethbtcObjectStream.readObject();
			if (ethObj instanceof List) {
				ethbtcSnapData = (Tuple2<List<CopyOnWriteArrayList<OrderData>>,Long>) ethObj;
			}
		}catch (Exception ex) {
			slf4jLogger.error(ex.getMessage());
		}finally {

			if (ethbtcFileStream != null) {
				try {
					ethbtcFileStream.close();
				} catch (IOException e) {
					slf4jLogger.error(e.getMessage());
				}
			}

			if (ethbtcObjectStream != null) {
				try {
					ethbtcObjectStream.close();
				} catch (IOException e) {
					slf4jLogger.error(e.getMessage());
				}
			}
		}
		if (ethbtcSnapData != null && ethbtcSnapData.f0 != null && ethbtcSnapData.f1 != null) {
			
			ETHBTCBIDASKCAL.bidOrderBook = ethbtcSnapData.f0.get(0);
			ETHBTCBIDASKCAL.askOrderBook = ethbtcSnapData.f0.get(1);
			ETHBTCBIDASKCAL.sequenceNo = ethbtcSnapData.f1;
			
		}

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.enableCheckpointing(2000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = HitBtcToADLMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the HitBtc-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Hitbtc-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_HitBtc_ETHBTC_Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new HitBtcEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream,"ethbtc");

		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);

		ethbtc_MidPointStream.addSink(new HitBtcEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new HitBtcEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		
		/**
		 * Adding the HitBtc-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Hitbtc-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_HitBtc_ETHBTC_Trade_Source").setParallelism(2);
		ethbtc_TradeStream.addSink(new HitBtcEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");


		/**
		 * Adding the HitBtc-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Hitbtc-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_HitBtc_ETHBTC_Ticker_Source").setParallelism(2);
		ethbtc_TickerStream.addSink(new HitBtcEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");

		
//
//		/**
//		 * Adding the HitBtc-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-LTCBTC-Order_Source").setParallelism(10);
//		ltcbtc_RawOrderStream.addSink(new HitBtcLtcbtcADLSink<String>()).setParallelism(25).name("LTCBTC_RawOrder_ADLSink");
////		ltcbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("LTCBTC_RawOrder_ESSink");
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);
//		ltcbtc_BestBidAskOrderStream.addSink(new HitBtcLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
////		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new HitBtcLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
////		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		/**
//		 * Adding the HitBtc-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-LTCBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new HitBtcLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
////		ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		/**
//		 * Adding the HitBtc-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-LTCBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-LTCBTC-Ticker_Source").setParallelism(2);
//		ltcbtc_TickerStream.addSink(new HitBtcLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
////		ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//
//		
//
//		/**
//		 * Adding the HitBtc-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-DASHBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-DSHBTC-Order_Source").setParallelism(10);
//		dshbtc_RawOrderStream.addSink(new HitBtcDshbtcADLSink<String>()).setParallelism(25).name("DSHBTC_RawOrder_ADLSink");
////		dshbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("DSHBTC_RawOrder_ESSink");
//		DataStream<String>  dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream);
//		dshbtc_BestBidAskOrderStream.addSink(new HitBtcDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_BestBidAsk_ADLSink");
////		dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		DataStream<String>  dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		dshbtc_MidPointStream.addSink(new HitBtcDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_Midpoint_ADLSink");
////		dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		/**
//		 * Adding the HitBtc-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-DASHBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-DSHBTC-Trade_Source").setParallelism(2);
//		dshbtc_TradeStream.addSink(new HitBtcDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Trade_ADLSink");
////		dshbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("DSHBTC_Trade_ESSink");
//		
//		/**
//		 * Adding the HitBtc-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-DASHBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-DSHBTC-Ticker_Source").setParallelism(2);
//		dshbtc_TickerStream.addSink(new HitBtcDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Ticker_ADLSink");
////		dshbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("DSHBTC_Ticker_ESSink");
//		
//		/**
//		 * Adding the HitBtc-XMRBTC-Order source to the execution environment
//		 */
//		DataStream<String> xmrbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-XMRBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-XMRBTC-Order_Source").setParallelism(10);
//		xmrbtc_RawOrderStream.addSink(new HitBtcXmrbtcADLSink<String>()).setParallelism(25).name("XMRBTC_RawOrder_ADLSink");
////		xmrbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("XMRBTC_RawOrder_ESSink");
//		DataStream<String>  xmrbtc_BestBidAskOrderStream = XMRBTCBIDASKCAL.calcBestBidAskStream(xmrbtc_RawOrderStream);
//		xmrbtc_BestBidAskOrderStream.addSink(new HitBtcXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_BestBidAsk_ADLSink");
////		xmrbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XMRBTC_BestBidAsk_ESSink");
//		DataStream<String>  xmrbtc_MidPointStream = XMRBTCMIDCAL.calculateMidPoint(xmrbtc_BestBidAskOrderStream);
//		xmrbtc_MidPointStream.addSink(new HitBtcXmrbtcADLSink<String>()).setParallelism(15).name("XMRBTC_Midpoint_ADLSink");
////		xmrbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XMRBTC_Midpoint_ESSink");
//		/**
//		 * Adding the HitBtc-XMRBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-XMRBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-XMRBTC-Trade_Source").setParallelism(2);
//		xmrbtc_TradeStream.addSink(new HitBtcXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Trade_ADLSink");
////		xmrbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("XMRBTC_Trade_ESSink");
//	
//		/**
//		 * Adding the HitBtc-XMRBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("Hitbtc-XMRBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_HitBtc-XMRBTC-Ticker_Source").setParallelism(2);
//		xmrbtc_TickerStream.addSink(new HitBtcXmrbtcADLSink<String>()).setParallelism(3).name("XMRBTC_Ticker_ADLSink");
////		xmrbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("XMRBTC_Ticker_ESSink");

		streamEnv.execute("HitBtc_Streaming_ADL_App");
		

	}

}
