package com.biz.coinbase2datalake;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.biz.coinbase2datalake.sink.CoinBaseEthbtcADLSink;
import com.biz.coinbase2datalake.sink.CoinBaseLtcbtcADLSink;
import com.biz.coinbase2datalake.util.BestBidAskCalculator;
import com.biz.coinbase2datalake.util.MidPointCalculator;
import com.biz.coinbase2datalake.util.OrderData;

public class CoinbaseToDataLakeMain implements Serializable
{
   
	private static final long serialVersionUID = 1L;

	private final static Logger slf4jLogger = LoggerFactory.getLogger(CoinbaseToDataLakeMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {


		
		 Tuple3<List<CopyOnWriteArrayList<OrderData>>,Long,Long> ethbtcSnapData = null;
		FileInputStream ethbtcFileStream = null;
		ObjectInputStream ethbtcObjectStream = null;
		try {
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/coinbaseADLethbtc.txt");
			ethbtcObjectStream = new ObjectInputStream(ethbtcFileStream);
			Object ethObj = ethbtcObjectStream.readObject();
			if (ethObj instanceof List) {
				ethbtcSnapData = ( Tuple3<List<CopyOnWriteArrayList<OrderData>>,Long,Long>) ethObj;
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
		if (ethbtcSnapData != null && ethbtcSnapData.f0 != null && ethbtcSnapData.f1 != null && ethbtcSnapData.f2 != null) {
			
			ETHBTCBIDASKCAL.bidOrderBook = ethbtcSnapData.f0.get(0);
			ETHBTCBIDASKCAL.askOrderBook = ethbtcSnapData.f0.get(1);
			ETHBTCBIDASKCAL.sequenceNo = ethbtcSnapData.f1;
			ETHBTCBIDASKCAL.randomNo = ethbtcSnapData.f2;
		}
		/*Tuple3<List<CopyOnWriteArrayList<OrderData>>,Long,Long> ltcbtcSnapData = null;
		FileInputStream ltcbtcFileStream = null;
		ObjectInputStream ltcbtcObjectStream = null;
		try {
			ltcbtcFileStream = new FileInputStream("/home/bizruntime/state/coinbaseADLltcbtc.txt");
			ltcbtcObjectStream = new ObjectInputStream(ltcbtcFileStream);
			Object ltcObj = ltcbtcObjectStream.readObject();
			if (ltcObj instanceof List) {
				ltcbtcSnapData = ( Tuple3<List<CopyOnWriteArrayList<OrderData>>,Long,Long>) ltcObj;
			}
		}catch (Exception ex) {
			slf4jLogger.error(ex.getMessage());
		}finally {

			if (ltcbtcFileStream != null) {
				try {
					ltcbtcFileStream.close();
				} catch (IOException e) {
					slf4jLogger.error(e.getMessage());
				}
			}

			if (ltcbtcFileStream != null) {
				try {
					ltcbtcFileStream.close();
				} catch (IOException e) {
					slf4jLogger.error(e.getMessage());
				}
			}
		}
		if (ltcbtcSnapData != null && ltcbtcSnapData.f0 != null && ltcbtcSnapData.f1 != null && ltcbtcSnapData.f2 != null) {
			
			LTCBTCBIDASKCAL.bidOrderBook = ltcbtcSnapData.f0.get(0);
			LTCBTCBIDASKCAL.askOrderBook = ltcbtcSnapData.f0.get(1);
			LTCBTCBIDASKCAL.sequenceNo = ltcbtcSnapData.f1;
			LTCBTCBIDASKCAL.randomNo = ltcbtcSnapData.f2;
		}*/
		final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.enableCheckpointing(5000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = CoinbaseToDataLakeMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the CoinBase-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("CoinBase-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_CoinBase_ETHBTC_Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new CoinBaseEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream,"ethbtc");
		ethbtc_BestBidAskOrderStream.addSink(new CoinBaseEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_BestBidAsk_ADLSink");
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);

		ethbtc_MidPointStream.addSink(new CoinBaseEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");


		/**
		 * Adding the CoinBase-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("CoinBase-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_CoinBase_ETHBTC_Trade_Source").setParallelism(2);
		ethbtc_TradeStream.addSink(new CoinBaseEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");

		
		/**
		 * Adding the CoinBase-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("CoinBase-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_CoinBase_ETHBTC_Ticker_Source").setParallelism(2);
		ethbtc_TickerStream.addSink(new CoinBaseEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");

		
		

//		/**
//		 * Adding the CoinBase-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("CoinBase-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_CoinBase-LTCBTC-Order_Source").setParallelism(5);
//		ltcbtc_RawOrderStream.addSink(new CoinBaseLtcbtcADLSink<String>()).name("LTCBTC_RawOrder_ADLSink").setParallelism(15);
//
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream,"ltcbtc");
//		ltcbtc_BestBidAskOrderStream.addSink(new CoinBaseLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
//
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new CoinBaseLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
//
//		
//		/**
//		 * Adding the CoinBase-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("CoinBase-LTCBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_CoinBase-LTCBTC-Trade_Source").setParallelism(1);
//		ltcbtc_TradeStream.addSink(new CoinBaseLtcbtcADLSink<String>()).setParallelism(2).name("LTCBTC_Trade_ADLSink");
//
//		
//		/**
//		 * Adding the CoinBase-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("CoinBase-LTCBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_CoinBase-LTCBTC-Ticker_Source").setParallelism(1);
//		ltcbtc_TickerStream.addSink(new CoinBaseLtcbtcADLSink<String>()).setParallelism(2).name("LTCBTC_Ticker_ADLSink");


		streamEnv.execute("CoinBase_Streaming_ADL_App");

	}

}
