package co.biz.exmo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import co.biz.exmo.sink.ExmoDshbtcADLSink;
import co.biz.exmo.sink.ExmoEthbtcADLSink;
import co.biz.exmo.sink.ExmoLtcbtcADLSink;
import co.biz.exmo.util.BestBidAskCalculator;
import co.biz.exmo.util.MidPointCalculator;
/**
 * @author Dhinesh Raja
 *
 */
public class ExmoToDatalakeMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(ExmoToDatalakeMain.class);
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
    	try {
    		kafkaPropStream = ExmoToDatalakeMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the Exmo-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Exmo-ETH_BTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_Exmo_ETHBTC_Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new ExmoEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);

		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);

		ethbtc_MidPointStream.addSink(new ExmoEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new ExmoEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		/**
		 * Adding the Exmo-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Exmo-ETH_BTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_Exmo_ETHBTC_Trade_Source").setParallelism(2);
		ethbtc_TradeStream.addSink(new ExmoEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Trade_ADLSink");



		/**
		 * Adding the Exmo-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Exmo-ETH_BTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_Exmo_ETHBTC_Ticker_Source").setParallelism(2);
		ethbtc_TickerStream.addSink(new ExmoEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Ticker_ADLSink");

		
//
//		/**
//		 * Adding the Exmo-LTCBTC-Order source to the execution environment
//		 */
//		DataStream<String> ltcbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Exmo-LTC_BTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Exmo-LTCBTC-Order_Source").setParallelism(10);
//
//		ltcbtc_RawOrderStream.addSink(new ExmoLtcbtcADLSink<String>()).setParallelism(25).name("LTCBTC_RawOrder_ADLSink");
////		ltcbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("LTCBTC_RawOrder_ESSink");
//		DataStream<String>  ltcbtc_BestBidAskOrderStream = LTCBTCBIDASKCAL.calcBestBidAskStream(ltcbtc_RawOrderStream);
//		ltcbtc_BestBidAskOrderStream.addSink(new ExmoLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_BestBidAsk_ADLSink");
////		ltcbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("LTCBTC_BestBidAsk_ESSink");
//		DataStream<String>  ltcbtc_MidPointStream = LTCBTCMIDCAL.calculateMidPoint(ltcbtc_BestBidAskOrderStream);
//		ltcbtc_MidPointStream.addSink(new ExmoLtcbtcADLSink<String>()).setParallelism(15).name("LTCBTC_Midpoint_ADLSink");
////		ltcbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("LTCBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Exmo-LTCBTC-Trade source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Exmo-LTC_BTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Exmo-LTCBTC-Trade_Source").setParallelism(2);
//		ltcbtc_TradeStream.addSink(new ExmoLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Trade_ADLSink");
//		//ltcbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("LTCBTC_Trade_ESSink");
//		/**
//		 * Adding the Exmo-LTCBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> ltcbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Exmo-LTC_BTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Exmo-LTCBTC-Ticker_Source").setParallelism(2);
//		ltcbtc_TickerStream.addSink(new ExmoLtcbtcADLSink<String>()).setParallelism(3).name("LTCBTC_Ticker_ADLSink");
//		//ltcbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("LTCBTC_Ticker_ESSink");
//
//		
//		/**
//		 * Adding the Exmo-DSHBTC-Order source to the execution environment
//		 */
//		DataStream<String> dshbtc_RawOrderStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Exmo-DASH_BTC-Order", new SimpleStringSchema(), properties),
//				"Kafka_Exmo-DSHBTC-Order_Source").setParallelism(10);
//		dshbtc_RawOrderStream.addSink(new ExmoDshbtcADLSink<String>()).setParallelism(25).name("DSHBTC_RawOrder_ADLSink");
////		dshbtc_RawOrderStream.addSink(ES_Sink.getESRawDataSink()).setParallelism(15).name("DSHBTC_RawOrder_ESSink");
//		DataStream<String>  dshbtc_BestBidAskOrderStream = DSHBTCBIDASKCAL.calcBestBidAskStream(dshbtc_RawOrderStream);
//		dshbtc_BestBidAskOrderStream.addSink(new ExmoDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_BestBidAsk_ADLSink");
////		dshbtc_BestBidAskOrderStream.addSink(ES_Sink.getESBestBidAskSink()).setParallelism(15).name("DSHBTC_BestBidAsk_ESSink");
//		DataStream<String>  dshbtc_MidPointStream = DSHBTCMIDCAL.calculateMidPoint(dshbtc_BestBidAskOrderStream);
//		dshbtc_MidPointStream.addSink(new ExmoDshbtcADLSink<String>()).setParallelism(15).name("DSHBTC_Midpoint_ADLSink");
////		dshbtc_MidPointStream.addSink(ES_Sink.getESMidpointSink()).setParallelism(15).name("DSHBTC_Midpoint_ESSink");
//		/**
//		 * Adding the Exmo-DSHBTC-Trade source to the execution environment
//		 */
//		DataStream<String> dshbtc_TradeStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Exmo-DASH_BTC-Trade", new SimpleStringSchema(), properties),
//				"Kafka_Exmo-DSHBTC-Trade_Source").setParallelism(2);
//		dshbtc_TradeStream.addSink(new ExmoDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Trade_ADLSink");
//		//dshbtc_TradeStream.addSink(ES_Sink.getESTradeSink()).setParallelism(3).name("DSHBTC_Trade_ESSink");
//		/**
//		 * Adding the Exmo-DSHBTC-Ticker source to the execution environment
//		 */
//		DataStream<String> dshbtc_TickerStream = env.addSource(
//				new FlinkKafkaConsumer010<String>("Exmo-DASH_BTC-Ticker", new SimpleStringSchema(), properties),
//				"Kafka_Exmo-DSHBTC-Ticker_Source").setParallelism(2);
//		dshbtc_TickerStream.addSink(new ExmoDshbtcADLSink<String>()).setParallelism(3).name("DSHBTC_Ticker_ADLSink");
//		//dshbtc_TickerStream.addSink(ES_Sink.getESTickerSink()).setParallelism(3).name("DSHBTC_Ticker_ESSink");
//		

		env.execute("Exmo_Streaming_Datalake_App");

	}

}
