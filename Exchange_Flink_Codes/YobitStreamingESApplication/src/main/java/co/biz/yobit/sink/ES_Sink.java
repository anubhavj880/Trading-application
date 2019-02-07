package co.biz.yobit.sink;

import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.biz.yobit.exceptions.ElasticSearchDataException;


/**
 * 
 * @author Dhinesh Raja
 *
 */
public class ES_Sink implements Serializable{
	private static final long serialVersionUID = 1L;

	private final static Logger slf4jLogger = LoggerFactory.getLogger(ES_Sink.class);

	public static ElasticsearchSinkFunction<String> getESSinkFunction(String indexname, String typename) {
		
		ElasticsearchSinkFunction<String> obj = new ElasticsearchSinkFunction<String>() {
			private static final long serialVersionUID = 1L;
			public IndexRequest createIndexRequest(String element) throws ElasticSearchDataException {
				JSONObject json = new JSONObject(element);
				IndexRequest indexRequest = null;
				if (indexname.equals("bestbidask") && (json.length() > 0) && (json.has("TimeStamp") && (json.getString("TimeStamp") != null)) && (json.has("BestBid")&& json.getJSONObject("BestBid").length() > 0) && (json.has("BestAsk")&& json.getJSONObject("BestAsk").length() > 0)) {
					indexRequest = Requests.indexRequest().index(indexname).type(typename).source(element);
					return indexRequest;
				}
				else if(indexname.equals("bestbidask") && (json.length() == 0))
				{
					throw new ElasticSearchDataException("The bestbidask data to elasticsearch index is empty");
				}
				else if(indexname.equals("midpoint") && (json.length() == 0))
				{
					throw new ElasticSearchDataException("The midpoint data to elasticsearch index is empty");
				}
				else if(indexname.equals("bestbidask") && (json.length() > 0) && (json.has("TimeStamp") && (json.getString("TimeStamp") == null || json.getString("TimeStamp").isEmpty())))
				{
					throw new ElasticSearchDataException("The bestbidask TimeStampdata to elasticsearch index is empty or null");
				}
				else if(indexname.equals("bestbidask") && (json.length() > 0) && (json.has("BestBid") && (json.getJSONObject("BestBid") == null || json.getJSONObject("BestBid").length() == 0)))
				{
					throw new ElasticSearchDataException("The bestbidask BestBiddata to elasticsearch index is empty or null");
				}
				else if(indexname.equals("bestbidask") && (json.length() > 0) && (json.has("BestAsk") && (json.getJSONObject("BestAsk") == null || json.getJSONObject("BestAsk").length() == 0)))
				{
					throw new ElasticSearchDataException("The bestbidask BestBiddata to elasticsearch index is empty or null");
				}
				else if(indexname.equals("midpoint") && (json.length() > 0)  && (json.has("TimeStamp") && (json.getString("TimeStamp") != null) && (json.has("MidPoint") && json.getJSONObject("MidPoint").length() > 0)))
				{
					indexRequest = Requests.indexRequest().index(indexname).type(typename).source(element);
					return indexRequest;
				}
				else if(indexname.equals("midpoint") && (json.length() > 0) && (json.has("TimeStamp") && (json.getString("TimeStamp") == null || json.getString("TimeStamp").isEmpty())))
				{
					throw new ElasticSearchDataException("The midpoint TimeStampdata to elasticsearch index is empty or null");
				}
				else if(indexname.equals("midpoint") && (json.length() > 0) && (json.has("MidPoint") && (json.getJSONObject("MidPoint") == null || json.getJSONObject("MidPoint").length() == 0)))
				{
					throw new ElasticSearchDataException("The midpoint index  MidPointdata to elasticsearch  is empty or null");
				}
				else
				{
					throw new ElasticSearchDataException("cannot index the data to es some unknown error has occeured");
				}
				
				
				
			}
			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				try {
					indexer.add(createIndexRequest(element));
				} catch (ElasticSearchDataException e) {
					slf4jLogger.info(e.getMessage());
					
				}
			}
		};
		return obj;
		
	}

}
