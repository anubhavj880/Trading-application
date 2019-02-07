package co.biz.cexio2datalake;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import co.biz.cexio2datalake.sink.CexioEthbtcADLSink;
import co.biz.cexio2datalake.util.BestBidAskCalculator;
import co.biz.cexio2datalake.util.MidPointCalculator;

public class CexioToDatalakeMain {
	private final static Logger slf4jLogger = LoggerFactory.getLogger(CexioToDatalakeMain.class);
	private final static BestBidAskCalculator ETHBTCBIDASKCAL = new BestBidAskCalculator(); 
	private final static MidPointCalculator ETHBTCMIDCAL = new MidPointCalculator();
	public static void main(String[] args) throws Exception {

		/**
		 * Getting the execution Environment
		 */
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);
		Properties kafkaProp = new Properties();
    	InputStream kafkaPropStream = null;
    	try {
    		kafkaPropStream = CexioToDatalakeMain.class.getClassLoader().getResourceAsStream("kafka.properties");
    		
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
		 * Adding the Cexio-ETHBTC-Order source to the execution environment
		 */
		DataStream<String> ethbtc_RawOrderStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Cex_io-ETHBTC-Order", new SimpleStringSchema(), kafkaProp),
				"Kafka_Cexio_ETHBTC_Order_Source").setParallelism(5);
		ethbtc_RawOrderStream.addSink(new CexioEthbtcADLSink<String>()).name("ETHBTC_RawOrder_ADLSink").setParallelism(1);

		DataStream<String>  ethbtc_BestBidAskOrderStream = ETHBTCBIDASKCAL.calcBestBidAskStream(ethbtc_RawOrderStream);

		DataStream<String>  ethbtc_MidPointStream = ETHBTCMIDCAL.calculateMidPoint(ethbtc_BestBidAskOrderStream);

		ethbtc_MidPointStream.addSink(new CexioEthbtcADLSink<String>()).setParallelism(1).name("ETHBTC_Midpoint_ADLSink");
		ethbtc_BestBidAskOrderStream.addSink(new CexioEthbtcADLSink<String>()).name("ETHBTC_BestBidAskOrder_ADLSink").setParallelism(1);
		
		/**
		 * Adding the Cexio-ETHBTC-Trade source to the execution environment
		 */
		DataStream<String> ethbtc_TradeStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Cex_io-ETHBTC-Trade", new SimpleStringSchema(), kafkaProp),
				"Kafka_Cexio_ETHBTC_Trade_Source").setParallelism(2);

		ethbtc_TradeStream.addSink(new CexioEthbtcADLSink<String>()).name("ETHBTC_Trade_ADLSink").setParallelism(1);
		/**
		 * Adding the Cexio-ETHBTC-Ticker source to the execution environment
		 */
		DataStream<String> ethbtc_TickerStream = env.addSource(
				new FlinkKafkaConsumer010<String>("Cex_io-ETHBTC-Ticker", new SimpleStringSchema(), kafkaProp),
				"Kafka_Cexio_ETHBTC_Ticker_Source").setParallelism(2);

		ethbtc_TickerStream.addSink(new CexioEthbtcADLSink<String>()).name("ETHBTC_Ticker_ADLSink").setParallelism(1);
		env.execute("Cexio_Streaming_Datalake_App");

	}

}
