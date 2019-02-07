

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author Dhinesh Raja
 *
 */
public class ES_Sink implements Serializable{
	private static final long serialVersionUID = 1L;
	private static ElasticsearchSink<String> obj =null;
	private static ElasticsearchSink<String> midpointsSinkObj =null;
	private static ElasticsearchSink<String> bestbidaskSinkObj =null;
	private static ElasticsearchSink<String> tickerSinkObj =null;
	private static ElasticsearchSink<String> tradeSinkObj =null;
	private final static Logger slf4jLogger = LoggerFactory.getLogger(ES_Sink.class);
		
	

	public static ElasticsearchSink<String> getESRawDataSink() throws Exception {
		if(obj == null){
			Map<String, String> config = new HashMap<String, String>();
			config.put("cluster.name", "TRADE-ES");
			config.put("bulk.flush.max.actions", "1");
			config.put("node.name", "node-1");
			List<InetSocketAddress> transportAddresses = new ArrayList<InetSocketAddress>();
			transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		obj =new ElasticsearchSink<String>(config, transportAddresses, ES_Sink.getObj("bitfinexorder", "bitfinexorder"));
		return obj;
		}
		else {
			return obj;
		}
	}
	public static ElasticsearchSink<String> getESBestBidAskSink() throws Exception {
		if(bestbidaskSinkObj == null){
		Map<String, String> config = new HashMap<String, String>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<InetSocketAddress>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		bestbidaskSinkObj =new ElasticsearchSink<String>(config, transportAddresses, ES_Sink.getObj("bestbidask", "bestbidask"));
		
		return bestbidaskSinkObj;
		}
		else {
			return bestbidaskSinkObj;
		}
	}
	public static ElasticsearchSink<String> getESMidpointSink() throws Exception {
		if(midpointsSinkObj == null){
		Map<String, String> config = new HashMap<String, String>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<InetSocketAddress>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		midpointsSinkObj =new ElasticsearchSink<String>(config, transportAddresses, ES_Sink.getObj("midpoint", "midpoint"));
		
		return midpointsSinkObj;
		}
		else {
			return midpointsSinkObj;
		}
	}
	public static ElasticsearchSink<String> getESTickerSink() throws Exception {
		if(tickerSinkObj == null){
		Map<String, String> config = new HashMap<String, String>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<InetSocketAddress>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		tickerSinkObj =new ElasticsearchSink<String>(config, transportAddresses, ES_Sink.getObj("ticker", "ticker"));
		return tickerSinkObj;
		}
		else {
			return tickerSinkObj;
		}
	}
	public static ElasticsearchSink<String> getESTradeSink() throws Exception {
		if(tradeSinkObj == null){
		Map<String, String> config = new HashMap<String, String>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<InetSocketAddress>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		tradeSinkObj =new ElasticsearchSink<String>(config, transportAddresses, ES_Sink.getObj("trade", "trade"));
		return tradeSinkObj;
		}
		else {
			return tradeSinkObj;
		}
	}
	private static ElasticsearchSinkFunction<String> getObj(final String indexname, final String typename) {
		
		ElasticsearchSinkFunction<String> obj = new ElasticsearchSinkFunction<String>() {
			private static final long serialVersionUID = 1L;
			public IndexRequest createIndexRequest(String element) throws Exception {
				JSONObject json = new JSONObject(element);
				IndexRequest indexRequest = null;
				if (json.length() > 0 ) {
					indexRequest = Requests.indexRequest().index(indexname).type(typename).source(element);
					return indexRequest;
				}
				else if(json.length() == 0 && indexname.equals("bestbidask"))
				{
					JSONObject bestBidAsk = new JSONObject();
					bestBidAsk.put("TimeStamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date(System.currentTimeMillis())));
					bestBidAsk.put("BestBid", new JSONObject());
					bestBidAsk.put("BestAsk", new JSONObject());
					return Requests.indexRequest().index(indexname).type(typename).source(bestBidAsk.toString());
					
				}
				else if(json.length() == 0 && indexname.equals("midpoint"))
				{
					JSONObject midpointJson = new JSONObject();
					midpointJson.put("TimeStamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date(System.currentTimeMillis())));
					midpointJson.put("MidPoint", new JSONObject());
				
					return Requests.indexRequest().index(indexname).type(typename).source(midpointJson.toString());
					
				}
				else if(json.length() == 0 && indexname.equals("trade"))
				{
					JSONObject tradeJson = new JSONObject();
					tradeJson.put("TimeStamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date(System.currentTimeMillis())));
					tradeJson.put("TradeBook", new JSONObject());
				
					return Requests.indexRequest().index(indexname).type(typename).source(tradeJson.toString());
					
				}
				else if(json.length() == 0 && indexname.equals("ticker"))
				{
					JSONObject tickerJson = new JSONObject();
					tickerJson.put("TimeStamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date(System.currentTimeMillis())));
					tickerJson.put("TickerBok", new JSONObject());
				
					return Requests.indexRequest().index(indexname).type(typename).source(tickerJson.toString());
					
				}
				else 
				{
					throw new Exception("Problem with the indexing , Check the elasticsearch and flink logs............................");
				}
				
			}
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				try {
					indexer.add(createIndexRequest(element));
				} catch (Exception e) {
					slf4jLogger.info(e.getMessage());
				}
			}
		};
		return obj;
		
	}

}
