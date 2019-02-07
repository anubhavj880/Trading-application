package co.biz.bitstamp.sink;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
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
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		obj =new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getObj("bitstamporder", "bitstamporder"));
		return obj;
		}
		else {
			return obj;
		}
	}
	public static ElasticsearchSink<String> getESBestBidAskSink() throws Exception {
		if(bestbidaskSinkObj == null){
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		bestbidaskSinkObj =new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getObj("bitstampbestbidask", "bitstampbestbidask"));
		return bestbidaskSinkObj;
		}
		else {
			return bestbidaskSinkObj;
		}
	}
	public static ElasticsearchSink<String> getESMidpointSink() throws Exception {
		if(midpointsSinkObj == null){
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		midpointsSinkObj =new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getObj("bitstampmidpoint", "bitstampmidpoint"));
		return midpointsSinkObj;
		}
		else {
			return midpointsSinkObj;
		}
	}
	public static ElasticsearchSink<String> getESTickerSink() throws Exception {
		if(tickerSinkObj == null){
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		tickerSinkObj =new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getObj("bitstampticker", "bitstampticker"));
		return tickerSinkObj;
		}
		else {
			return tickerSinkObj;
		}
	}
	public static ElasticsearchSink<String> getESTradeSink() throws Exception {
		if(tradeSinkObj == null){
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "TRADE-ES");
		config.put("bulk.flush.max.actions", "1");
		config.put("node.name", "node-1");
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.1.159"), 9300));
		tradeSinkObj =new ElasticsearchSink<>(config, transportAddresses, ES_Sink.getObj("bitstamptrade", "bitstamptrade"));
		return tradeSinkObj;
		}
		else {
			return tradeSinkObj;
		}
	}
	private static ElasticsearchSinkFunction<String> getObj(String indexname, String typename) {
		
		ElasticsearchSinkFunction<String> obj = new ElasticsearchSinkFunction<String>() {
			private static final long serialVersionUID = 1L;
			public IndexRequest createIndexRequest(String element) {
				return Requests.indexRequest().index(indexname).type(typename).source(element);
			}
			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				indexer.add(createIndexRequest(element));
			}
		};
		return obj;
		
	}

}
