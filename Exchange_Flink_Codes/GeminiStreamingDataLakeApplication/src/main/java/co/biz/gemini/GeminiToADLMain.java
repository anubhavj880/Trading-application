package co.biz.gemini;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.HdfsFileStatusProto.FileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import co.biz.gemini.sink.GeminiEthbtcADLSink;
import co.biz.gemini.util.BestBidAskCalculator;
import co.biz.gemini.util.MidPointCalculator;
import co.biz.gemini.util.OrderData;

/**
 * 
 * @author Dhinesh Raja
 *
 */
public class GeminiToADLMain {
	@SuppressWarnings("unused")
	private final static Logger slf4jLogger = LoggerFactory.getLogger(GeminiToADLMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		
		List<List<OrderData>> ethbtcSnapData = null;
		FileInputStream ethbtcFileStream = null;
		ObjectInputStream ethbtcObjectStream = null;
		try {
			ethbtcFileStream = new FileInputStream("/home/bizruntime/state/geminiADLethbtc.txt");
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
		streamEnv.enableCheckpointing(5000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = GeminiToADLMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the GEMINI-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Gemini-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_GEMINI-ETHBTC-Order_Source").setParallelism(5);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream,"ethbtc");

		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);

		ethbtc_MidPointStream.addSink(new GeminiEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new GeminiEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		ethbtc_RawOrderStream.addSink(new GeminiEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		/**
		 * Adding the GEMINI-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Gemini-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_GEMINI-ETHBTC-Trade_Source").setParallelism(1);
		ethbtc_TradeStream.addSink(new GeminiEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");

		
		/**
		 * Adding the GEMINI-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = streamEnv.addSource(
				new FlinkKafkaConsumer010<String>("Gemini-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_GEMINI-ETHBTC-Ticker_Source").setParallelism(1);
		ethbtc_TickerStream.addSink(new GeminiEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");


		streamEnv.execute("GEMINI_Streaming_ADL_App");

	}

}
