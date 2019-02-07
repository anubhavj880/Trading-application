package co.biz.yobit;

import java.io.IOException;
import java.io.InputStream;
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
import co.biz.yobit.sink.ES_Sink;
import co.biz.yobit.util.BestBidAskCalculator;
import co.biz.yobit.util.MidPointCalculator;

public class YobitToESMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(YobitToESMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator LTCBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator LTCBTCMIDCAL = new MidPointCalculator();
	private final static BestBidAskCalculator DSHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator DSHBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	Properties esProp = new Properties();
    	InputStream esPropStream = null;
    	

    	try {
    		kafkaPropStream = YobitToESMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		esPropStream = YobitToESMain.class.getClassLoader().getResourceAsStream("es.properties");
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
		 * Adding the Yobit-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Yobit-ETH_BTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_Yobit_ETHBTC_Order_Source").setParallelism(5);


		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);
		ethbtc_BestBidAskOrderStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("bestbidask", "bestbidask"))).setParallelism(15).name("ETHBTC_BestBidAsk_ESSink");
		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);
		ethbtc_MidPointStream.addSink(new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getESSinkFunction("midpoint", "midpoint"))).setParallelism(15).name("ETHBTC_Midpoint_ESSink");

		
		
//		/**
//		 * Adding the Yobit-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Yobit-LTC_BTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Yobit-LTCBTC-Order_Source").setParallelism(10);
//		ltcbtc_RawOrderStream.addSink(new YobitLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_RawOrder_ADLSink");
////		ltcbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("LTCBTC_RawOrder_ESSink");
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);
//		ltcbtc_BestBidAskOrderStream.addSink(new YobitLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
////		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new YobitLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
////		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Yobit-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Yobit-LTC_BTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Yobit-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new YobitLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
////		ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		/**
//		 * Adding the Yobit-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Yobit-LTC_BTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Yobit-LTCBTC-Ticker_Source").setParallelism(2);
//		ltcbtc_TickerStream.addSink(new YobitLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
////		ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//
//
//
//		/**
//		 * Adding the Yobit-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Yobit-DASH_BTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Yobit-DSHBTC-Order_Source").setParallelism(10);
//		dshbtc_RawOrderStream.addSink(new YobitDshbtcADLSink<String>()).setParallelism(25).name("DSHBTC_RawOrder_ADLSink");
////		dshbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("DSHBTC_RawOrder_ESSink");
//		DataStream<String>  dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream);
//		dshbtc_BestBidAskOrderStream.addSink(new YobitDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_BestBidAsk_ADLSink");
////		dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		DataStream<String>  dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		dshbtc_MidPointStream.addSink(new YobitDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_Midpoint_ADLSink");
////		dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Yobit-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Yobit-DASH_BTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Yobit-DSHBTC-Trade_Source").setParallelism(2);
//		dshbtc_TradeStream.addSink(new YobitDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Trade_ADLSink");
////		dshbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("DSHBTC_Trade_ESSink");
//		/**
//		 * Adding the Yobit-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Yobit-DASH_BTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Yobit-DSHBTC-Ticker_Source").setParallelism(2);
//		dshbtc_TickerStream.addSink(new YobitDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Ticker_ADLSink");
////		dshbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("DSHBTC_Ticker_ESSink");
//


		env.execute("Yobit_Streaming_ES_App");

	}

}
