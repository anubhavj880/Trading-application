package co.biz.coinbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.biz.coinbase.sink.ES_Sink;
import co.biz.coinbase.util.BestBidAskCalculator;
import co.biz.coinbase.util.MidPointCalculator;
import co.biz.coinbase.util.OrderData;

public class CoinbaseToESMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(CoinbaseToESMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {


		
		Tuple3<List<CopyOnWriteArrayList<OrderData>>,Long,Long> ethbtcSnapData = null;
		FileInputStream ethbtcFileStream = null;
		ObjectInputStream ethbtcObjectStream = null;
		try {
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/coinbaseESethbtc.txt");
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
			ltcbtcFileStream = new FileInputStream("/home/bizruntime/state/coinbaseESltcbtc.txt");
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
		streamEnv.enableCheckpointing(2000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	Properties esProp = new Properties();
    	InputStream esPropStream = null;
    	

    	try {
    		kafkaPropStream = CoinbaseToESMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		esPropStream = CoinbaseToESMain.class.getClassLoader().getResourceAsStream("es.properties");
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
		 * Adding the CoinBase-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("CoinBase-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_CoinBase_ETHBTC_Order_Source").setParallelism(5);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream,"ethbtc");
		ethbtc_BestBidAskOrderStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("coinbasebestbidask", "coinbasebestbidask"))).setParallelism(15).name("ETHBTC_BestBidAsk_ESSink");
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);
		ethbtc_MidPointStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("midpoint", "midpoint"))).setParallelism(15).name("ETHBTC_Midpoint_ESSink");

		

//		/**
//		 * Adding the CoinBase-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = streamEnv.addSource(
//				new FlinkKafkaConsumer010<String>("CoinBase-LTCBTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_CoinBase-LTCBTC-Order_Source").setParallelism(5);
//
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream,"ltcbtc");
//
//		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//
//		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
		
	
		streamEnv.execute("CoinBase_Streaming_ES_App");

	}

}
