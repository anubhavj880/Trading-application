package com.biz.bitfinex2datalake;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.biz.bitfinex2datalake.sink.BitfinexDshBtcADLSink;
import com.biz.bitfinex2datalake.sink.BitfinexEthBtcADLSink;
import com.biz.bitfinex2datalake.sink.BitfinexLtcBtcADLSink;
import com.biz.bitfinex2datalake.sink.BitfinexXmrBtcADLSink;
import com.biz.bitfinex2datalake.util.BestBidAskCalculator;
import com.biz.bitfinex2datalake.util.MidPointCalculator;
import com.biz.bitfinex2datalake.util.OrderData;


/**
 * @author Dhinesh Raja
 *
 */
public class BitfinexToDataLakeMain implements Serializable
{
	@SuppressWarnings("unused")
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitfinexToDataLakeMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator("ethbtc");
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator("ltcbtc");
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator DSHBTCBIDASKCAL = new BestBidAskCalculator("dshbtc");
	private final static MidPointCalculator DSHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XMRBTCBIDASKCAL = new BestBidAskCalculator("xmrbtc");
	private final static MidPointCalculator XMRBTCMIDCAL = new MidPointCalculator();

	@SuppressWarnings({ "unchecked", "deprecation" })
	public static void main(String[] args) throws Exception {

		List<CopyOnWriteArrayList<OrderData>> ethbtcSnapData = null;
		List<List<OrderData>> ltcbtcSnapData = null;
		List<List<OrderData>> dshbtcSnapData = null;
		List<List<OrderData>> xmrbtcSnapData = null;
		FileInputStream ethbtcFileStream = null;
		ObjectInputStream ethbtcObjectStream = null;
		FileInputStream ltcbtcFileStream = null;
		ObjectInputStream ltcbtcObjectStream = null;
		FileInputStream dshbtcFileStream = null;
		ObjectInputStream dshbtcObjectStream = null;
		FileInputStream xmrbtcFileStream = null;
		ObjectInputStream xmrbtcObjectStream = null;

		try {
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexADLethbtc.txt");
			ethbtcObjectStream = new ObjectInputStream(ethbtcFileStream);
			Object ethObj = ethbtcObjectStream.readObject();
			if (ethObj instanceof List) {
				ethbtcSnapData =  (List<CopyOnWriteArrayList<OrderData>>) ethObj;
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
		if (ethbtcSnapData != null ) {
			if(ethbtcSnapData.size() > 0)
			{
			ETHBTCBIDASKCAL.bidOrderBook = ethbtcSnapData.get(0);
			ETHBTCBIDASKCAL.askOrderBook = ethbtcSnapData.get(1);
			}
		}
//			try {
//			ltcbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexADLltcbtc.txt");
//			ltcbtcObjectStream = new ObjectInputStream(ltcbtcFileStream);
//			Object ltcObj = ltcbtcObjectStream.readObject();
//			if (ltcObj instanceof List) {
//				ltcbtcSnapData = (List<List<OrderData>>) ltcObj;
//			}
//			}catch (Exception ex) {
//				slf4jLogger.error(ex.getMessage());
//			} finally {
//
//
//				if (ltcbtcFileStream != null) {
//					try {
//						ltcbtcFileStream.close();
//					} catch (IOException e) {
//						slf4jLogger.error(e.getMessage());
//					}
//				}
//
//				if (ltcbtcObjectStream != null) {
//					try {
//						ltcbtcObjectStream.close();
//					} catch (IOException e) {
//						slf4jLogger.error(e.getMessage());
//					}
//				}
//			}
//			if (ltcbtcSnapData != null ) {
//				if(!ltcbtcSnapData.isEmpty())
//				{
//				LTCBTCBIDASKCAL.bidOrderBook = ltcbtcSnapData.get(0);
//				LTCBTCBIDASKCAL.askOrderBook = ltcbtcSnapData.get(1);
//				}
//			}
//			
//			try {
//			dshbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexADLdshbtc.txt");
//			dshbtcObjectStream = new ObjectInputStream(dshbtcFileStream);
//			Object dshObj = dshbtcObjectStream.readObject();
//			if (dshObj instanceof List) {
//				dshbtcSnapData = (List<List<OrderData>>) dshObj;
//			}
//			}catch (Exception ex) {
//				slf4jLogger.error(ex.getMessage());
//			} finally {
//
//				if (dshbtcFileStream != null) {
//					try {
//						dshbtcFileStream.close();
//					} catch (IOException e) {
//						slf4jLogger.error(e.getMessage());
//					}
//				}
//
//				if (dshbtcObjectStream != null) {
//					try {
//						dshbtcObjectStream.close();
//					} catch (IOException e) {
//						slf4jLogger.error(e.getMessage());
//					}
//				}
//			}
//			if (dshbtcSnapData != null ) {
//				if(!dshbtcSnapData.isEmpty())
//				{
//				DSHBTCBIDASKCAL.bidOrderBook = dshbtcSnapData.get(0);
//				DSHBTCBIDASKCAL.askOrderBook = dshbtcSnapData.get(1);
//				}
//			}
//			
//			try {
//			xmrbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexADLxmrbtc.txt");
//			xmrbtcObjectStream = new ObjectInputStream(xmrbtcFileStream);
//			Object xmrObj = xmrbtcObjectStream.readObject();
//			if (xmrObj instanceof List) {
//				xmrbtcSnapData = (List<List<OrderData>>) xmrObj;
//			}
//
//		} catch (Exception ex) {
//			slf4jLogger.error(ex.getMessage());
//		} finally {
//			if (xmrbtcFileStream != null) {
//				try {
//					xmrbtcFileStream.close();
//				} catch (IOException e) {
//					slf4jLogger.error(e.getMessage());
//				}
//			}
//
//			if (xmrbtcObjectStream != null) {
//				try {
//					xmrbtcObjectStream.close();
//				} catch (IOException e) {
//					slf4jLogger.error(e.getMessage());
//				}
//			}
//
//		}
//		if (xmrbtcSnapData != null ) {
//			if(!xmrbtcSnapData.isEmpty())
//			{
//			XMRBTCBIDASKCAL.bidOrderBook = xmrbtcSnapData.get(0);
//			XMRBTCBIDASKCAL.askOrderBook = xmrbtcSnapData.get(1);
//			}
//		}

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.enableCheckpointing(3000);

		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;

    	try {
    		kafkaPropStream = BitfinexToDataLakeMain.class.getClassLoader().getResourceAsStream("kafka.properties");
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
		 * Adding the BitFinex-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_BitFinex-ETHBTC-Order_Source").setParallelism(5);
		

		DataStream<String> ethbtc_BestBidAskOrderStreamSnapshot = ETHBTCBIDASKCAL
				.calcBestBidAskStream(ethbtc_RawOrderStream);
		DataStream<String> ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStreamSnapshot);
		ethbtc_MidPointStream.addSink(new BitfinexEthBtcADLSink<String>()).setParallelism(1)
				.name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStreamSnapshot.addSink(new BitfinexEthBtcADLSink<String>())
				.name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		ethbtc_RawOrderStream.addSink(new BitfinexEthBtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink")
		.setParallelism(1);

		/**
		 * Adding the BitFinex-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_BitFinex-ETHBTC-Trade_Source").setParallelism(1);
		ethbtc_TradeStream.addSink(new BitfinexEthBtcADLSink<String>()).name("ETHBTC_Trade_ADLSink").setParallelism(1);


		/**
		 * Adding the BitFinex-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_BitFinex-ETHBTC-Ticker_Source").setParallelism(1);
		ethbtc_TickerStream.addSink(new BitfinexEthBtcADLSink<String>()).name("ETHBTC_Ticker_ADLSink")
				.setParallelism(1);


//		/**
//		 * Adding the BitFinex-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-LTCBTC-Order_Source").setParallelism(5);
//		ltcbtc_RawOrderStream.addSink(new BitfinexLtcBtcADLSink<String>()).name("LTCBTC_RawOrder_ADLSink")
//				.setParallelism(15);
//		DataStream<String> ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream,"ltcbtc");
//		ltcbtc_BestBidAskOrderStream.addSink(new BitfinexLtcBtcADLSink<String>()).name("LTCBTC_BestBidAskOrder_ADLSink")
//				.setParallelism(15);
//		DataStream<String> ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new BitfinexLtcBtcADLSink<String>()).setParallelism(15)
//				.name("LTCBTC_Midpoint_ADLSink");
//
//		/**
//		 * Adding the BitFinex-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-LTCBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-LTCBTC-Trade_Source").setParallelism(1);
//		ltcbtc_TradeStream.addSink(new BitfinexLtcBtcADLSink<String>()).name("LTCBTC_Trade_ADLSink").setParallelism(2);
//		/**
//		 * Adding the BitFinex-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-LTCBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-LTCBTC-Ticker_Source").setParallelism(1);
//		ltcbtc_TickerStream.addSink(new BitfinexLtcBtcADLSink<String>()).name("LTCBTC_Ticker_ADLSink")
//				.setParallelism(2);
//
//		/**
//		 * Adding the BitFinex-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-DSHBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-DSHBTC-Order_Source").setParallelism(5);
//		dshbtc_RawOrderStream.addSink(new BitfinexDshBtcADLSink<String>()).name("DSHBTC_RawOrder_ADLSink")
//				.setParallelism(15);
//		DataStream<String> dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream,"dshbtc");
//		dshbtc_BestBidAskOrderStream.addSink(new BitfinexDshBtcADLSink<String>()).name("DSHBTC_BestBidAskOrder_ADLSink")
//				.setParallelism(15);
//		DataStream<String> dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		dshbtc_MidPointStream.addSink(new BitfinexDshBtcADLSink<String>()).setParallelism(15)
//				.name("DSHBTC_Midpoint_ADLSink");
//
//		/**
//		 * Adding the BitFinex-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-DSHBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-DSHBTC-Trade_Source").setParallelism(1);
//		dshbtc_TradeStream.addSink(new BitfinexDshBtcADLSink<String>()).name("DSHBTC_Trade_ADLSink").setParallelism(2);
//
//		/**
//		 * Adding the BitFinex-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-DSHBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-DSHBTC-Ticker_Source").setParallelism(1);
//		dshbtc_TickerStream.addSink(new BitfinexDshBtcADLSink<String>()).name("DSHBTC_Ticker_ADLSink")
//				.setParallelism(2);
//
//		/**
//		 * Adding the BitFinex-XMRBTC-Order source to the execution environment
//		 */
//		DataStream<String> xmrbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-XMRBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-XMRBTC-Order_Source").setParallelism(5);
//		xmrbtc_RawOrderStream.addSink(new BitfinexXmrBtcADLSink<String>()).name("XMRBTC_RawOrder_ADLSink")
//				.setParallelism(15);
//		DataStream<String> xmrbtc_BestBidAskOrderStream = XMRBTCBIDASKCAL.calcBestBidAskStream(xmrbtc_RawOrderStream,"xmrbtc");
//		xmrbtc_BestBidAskOrderStream.addSink(new BitfinexXmrBtcADLSink<String>()).name("XMRBTC_BestBidAskOrder_ADLSink")
//				.setParallelism(15);
//		DataStream<String> xmrbtc_MidPointStream = XMRBTCMIDCAL.calculateMidPoint(xmrbtc_BestBidAskOrderStream);
//		xmrbtc_MidPointStream.addSink(new BitfinexXmrBtcADLSink<String>()).setParallelism(15)
//				.name("XMRBTC_Midpoint_ADLSink");
//		/**
//		 * Adding the BitFinex-XMRBTC-Trade source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TradeStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-XMRBTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-XMRBTC-Trade_Source").setParallelism(1);
//		xmrbtc_TradeStream.addSink(new BitfinexXmrBtcADLSink<String>()).name("XMRBTC_Trade_ADLSink").setParallelism(2);
//		/**
//		 * Adding the BitFinex-XMRBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> xmrbtc_TickerStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-XMRBTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-XMRBTC-Ticker_Source").setParallelism(1);
//		xmrbtc_TickerStream.addSink(new BitfinexXmrBtcADLSink<String>()).name("XMRBTC_Ticker_ADLSink").setParallelism(2);
		streamEnv.execute("Bitfinex_Streaming_DataLake_APP");

	}

}
