package co.biz.bitfinex;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import co.biz.bitfinex.sink.ES_Sink;
import co.biz.bitfinex.util.BestBidAskCalculator;
import co.biz.bitfinex.util.MidPointCalculator;
import co.biz.bitfinex.util.OrderData;


/**
 * 
 * @author Dhinesh Raja
 *
 */
public class BitfinexToESMain implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final static Logger slf4jLogger = LoggerFactory.getLogger(BitfinexToESMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator("ethbtc");
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator("ltcbtc");
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator DSHBTCBIDASKCAL = new BestBidAskCalculator("dshbtc");
	private final static MidPointCalculator DSHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator XMRBTCBIDASKCAL = new BestBidAskCalculator("xmrbtc");
	private final static MidPointCalculator XMRBTCMIDCAL = new MidPointCalculator();

	@SuppressWarnings("unchecked")
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
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexESethbtc.txt");
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
//			ltcbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexESltcbtc.txt");
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
//			dshbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexESdshbtc.txt");
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
//			xmrbtcFileStream = new FileInputStream("/home/bizruntime/state/bitfinexESxmrbtc.txt");
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
		streamEnv.enableCheckpointing(2000);
		
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
		Properties esProp = new Properties();
    	InputStream esPropStream = null;
    	

    	try {
    		kafkaPropStream = BitfinexToESMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		esPropStream = BitfinexToESMain.class.getClassLoader().getResourceAsStream("es.properties");
    		if(kafkaPropStream==null){
    	            slf4jLogger.error("Sorry, unable to find " + "kafka.properties");
    		    return;
    		}
    		if(esPropStream==null){
    			 slf4jLogger.error("Sorry, unable to find " + "es.properties");
		    return;
		}

    		kafkaProp.load(kafkaPropStream);
    		esProp.load(esPropStream);
    		
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
        	if(esPropStream!=null){
        		try {
        			esPropStream.close();
			} catch (IOException e) {
				slf4jLogger.error(e.getMessage());
			}
        	}
        }

    	Map<String, String> config = new HashMap<>();
		config.put("cluster.name", esProp.getProperty("cluster.name"));
		config.put("bulk.flush.max.actions", esProp.getProperty("bulk.flush.max.actions"));
		config.put("node.name", esProp.getProperty("node.name"));
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName(esProp.getProperty("socketAddress")), 9300));
		
		
		/**
		 * Adding the BitFinex-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("BitFinex-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_BitFinex-ETHBTC-Order_Source").setParallelism(5);
		

		DataStream<String> ethbtc_BestBidAskOrderStreamSnapshot = ETHBTCBIDASKCAL
				.calcBestBidAskStream(ethbtc_RawOrderStream);
		ethbtc_BestBidAskOrderStreamSnapshot.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("bestbidask", "bestbidask"))).setParallelism(15).name("ETHBTC_BestBidAsk_ESSink");
		DataStream<String> ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStreamSnapshot);
		ethbtc_MidPointStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("midpoint", "midpoint"))).setParallelism(15).name("ETHBTC_Midpoint_ESSink");
		
		
		
		


	
//
//		/**
//		 * Adding the BitFinex-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-LTCBTC-Order_Source").setParallelism(5);
//		DataStream<String> ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream,"ltcbtc");
//    	 ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String> ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		 ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		
//
//		
//		/**
//		 * Adding the BitFinex-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-DSHBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-DSHBTC-Order_Source").setParallelism(5);
//		DataStream<String> dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream,"dshbtc");
//	   dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		DataStream<String> dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		 dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		
//		
//		/**
//		 * Adding the BitFinex-XMRBTC-Order source to the execution environment
//		 */
//		DataStream<String> xmrbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("BitFinex-XMRBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_BitFinex-XMRBTC-Order_Source").setParallelism(5);
//		
//		DataStream<String> xmrbtc_BestBidAskOrderStream = XMRBTCBIDASKCAL.calcBestBidAskStream(xmrbtc_RawOrderStream,"xmrbtc");
//		
//		 xmrbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("XMRBTC_BestBidAsk_ESSink");
//		DataStream<String> xmrbtc_MidPointStream = XMRBTCMIDCAL.calculateMidPoint(xmrbtc_BestBidAskOrderStream);
//		 xmrbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("XMRBTC_Midpoint_ESSink");
//		
		
		streamEnv.execute("BITFINEX_Streaming_ES_APP");

	}

}
