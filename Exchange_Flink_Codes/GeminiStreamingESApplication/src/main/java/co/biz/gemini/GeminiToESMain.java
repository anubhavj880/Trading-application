package co.biz.gemini;

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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.biz.gemini.sink.ES_Sink;
import co.biz.gemini.util.BestBidAskCalculator;
import co.biz.gemini.util.MidPointCalculator;
import co.biz.gemini.util.OrderData;

/**
 * 
 * @author Dhinesh Raja
 *
 */
public class GeminiToESMain {
	@SuppressWarnings("unused")
	private final static Logger slf4jLogger = LoggerFactory.getLogger(GeminiToESMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		List<List<OrderData>> ethbtcSnapData = null;
		FileInputStream ethbtcFileStream = null;
		ObjectInputStream ethbtcObjectStream = null;
		try {
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/geminiESethbtc.txt");
			ethbtcObjectStream = new ObjectInputStream(ethbtcFileStream);
			Object ethObj = ethbtcObjectStream.readObject();
			if (ethObj instanceof List) {
				ethbtcSnapData = (List<List<OrderData>>) ethObj;
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
			if(!ethbtcSnapData.isEmpty())
			{
			ETHBTCBIDASKCAL.bidOrderBook = ethbtcSnapData.get(0);
			ETHBTCBIDASKCAL.askOrderBook = ethbtcSnapData.get(1);
			}
		}
		

		final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.enableCheckpointing(2000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	Properties esProp = new Properties();
    	InputStream esPropStream = null;
    	

    	try {
    		kafkaPropStream = GeminiToESMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		esPropStream = GeminiToESMain.class.getClassLoader().getResourceAsStream("es.properties");
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
		 * Adding the GEMINI-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Gemini-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_GEMINI-ETHBTC-Order_Source").setParallelism(5);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream,"ethbtc");
		ethbtc_BestBidAskOrderStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("bestbidask", "bestbidask"))).setParallelism(15).name("ETHBTC_BestBidAsk_ESSink");
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);
		ethbtc_MidPointStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("midpoint", "midpoint"))).setParallelism(15).name("ETHBTC_Midpoint_ESSink");


		streamEnv.execute("GEMINI_Streaming_App");

	}

}
