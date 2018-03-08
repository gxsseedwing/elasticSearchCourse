package com.gx.fullsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class IndexSearchBase {

	public static TransportClient client;

	static {
		Settings settings = Settings.builder().put("cluster.name", "linkdood_index").put("client.transport.sniff", true)
				.build();

		// elasticSearch Version 2.3.3
		// client = TransportClient.builder().settings(settings).build()
		// .addTransportAddress(new
		// InetSocketTransportAddress(InetAddress.getByName("192.168.81.73"), 9300));

		// elasticSearch Version 5.6.4
		client = new PreBuiltTransportClient(settings)
				.addTransportAddresses(new InetSocketTransportAddress(InetAddresses.forString("192.168.81.73"), 9300));
	}

	/**
	 * 根据ID获取索引
	 * 
	 * @param indexName
	 * @param indexType
	 * @param id
	 * @return
	 */
	public Map<String, Object> searchDocumentById(String indexName, String indexType, String id) {
		GetResponse response = client.prepareGet(indexName, indexType, id).execute().actionGet();
		if (null == response) {
			return null;
		}
		return response.getSource();
	}

	/**
	 * 分页查询索引
	 * 
	 * @param indexName
	 * @param indexType
	 * @param queryBuilder
	 * @return
	 */
	public DocumentResult searchDocumentByPage(String indexName, String indexType, QueryBuilder queryBuilder, int start,
			int size, List<SortBuilder> sorts) {

		DocumentResult result = new DocumentResult();
		SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(indexName)// 索引名
				.setTypes(indexType)// 索引类型
				.setFrom(start)// 页码
				.setSize(size)// 页大小
				.setQuery(queryBuilder);// 条件

		// 设置排序
		if (null != sorts) {
			for (SortBuilder sortBuilder : sorts) {
				searchRequest.addSort(sortBuilder);
			}
		}
		// 执行查询
		SearchResponse searchResponse = searchRequest.execute().actionGet();

		List<Map<String, Object>> resultList = new ArrayList<>();
		long totalCount = searchResponse.getHits().getTotalHits();
		for (SearchHit hit : searchResponse.getHits().hits()) {
			resultList.add(hit.getSource());// 获取结果
		}
		result.setTotalCount(totalCount);
		result.setDocumentList(resultList);
		return result;
	}

	/**
	 * 滚动查询(首次)
	 * 
	 * @param indexName
	 * @param indexType
	 * @param queryBuilder
	 * @param time
	 * @param size
	 * @param sorts
	 * @return
	 */
	public DocumentResult searchDocumentBySrocl(String indexName, String indexType, QueryBuilder queryBuilder,
			long time, int size, List<SortBuilder> sorts) {
		DocumentResult result = new DocumentResult();
		SearchRequestBuilder searchRequest = client.prepareSearch().setIndices(indexName).setTypes(indexType)
				.setScroll(new TimeValue(time))// 设置超时时间
				.setSize(size).setQuery(queryBuilder);
		System.out.println(queryBuilder.toString());
		if (null != sorts) {
			for (SortBuilder sortBuilder : sorts) {
				searchRequest.addSort(sortBuilder);
			}
		}

		SearchResponse searchResponse = searchRequest.execute().actionGet();
		List<Map<String, Object>> resultList = new ArrayList<>();
		long totalCount = searchResponse.getHits().getTotalHits();
		for (SearchHit hit : searchResponse.getHits().hits()) {
			resultList.add(hit.getSourceAsMap());
		}
		result.setTotalCount(totalCount);
		result.setScrollID(searchResponse.getScrollId());// 获取滚动ID
		result.setDocumentList(resultList);

		return result;
	}

	/**
	 * 滚动查询(非首次)
	 * 
	 * @param time
	 * @param scrollId
	 * @return
	 */
	public DocumentResult searchDocumentBySrocl(long time, String scrollId) {
		DocumentResult result = new DocumentResult();
		SearchResponse searchResponse = client.prepareSearchScroll(scrollId).setScroll(new TimeValue(time)).execute()
				.actionGet();
		List<Map<String, Object>> resultList = new ArrayList<>();
		long totalCount = searchResponse.getHits().getTotalHits();
		for (SearchHit hit : searchResponse.getHits().hits()) {
			resultList.add(hit.getSource());
		}
		result.setScrollID(searchResponse.getScrollId());
		result.setDocumentList(resultList);
		
//		client.prepareIndex().setIndex("").setType("").setId("");
		
		return result;
	}

	/**
	 * 分页查询索引 
	 * @param indexName
	 * @param indexType
	 * @param queryBuilder
	 * @return
	 */
	public DocumentResult searchDocumentByAgg(String indexName, String indexType, QueryBuilder queryBuilder, int start, int size, List<SortBuilder> sorts) {

		DocumentResult result = new DocumentResult();
		SearchRequestBuilder searchRequest = client.prepareSearch()
				.setIndices(indexName)//索引名
				.setTypes(indexType)//索引类型
				.setFrom(start)//页码
				.setSize(size)//页大小
				.setQuery(queryBuilder)
				.addAggregation(AggregationBuilders.filter("userFilter", QueryBuilders.rangeQuery("userID").gt("2"))
						.subAggregation(AggregationBuilders.avg("userIDavg").field("userID")));//条件
		
//		AggregationBuilders.filter("agg1", QueryBuilders.termQuery("userID", "1")).subAggregation(AggregationBuilders.max("agg1").field("userID"));

		//设置排序
		if(null != sorts) 
		{
			for (SortBuilder sortBuilder : sorts) 
			{
				searchRequest.addSort(sortBuilder);
			}
		}
		//执行查询
		SearchResponse searchResponse = searchRequest.execute().actionGet();
		
		Filter userFilter = searchResponse.getAggregations().get("userFilter");	
		Avg avg = userFilter.getAggregations().get("userIDavg");
		
		System.out.println(avg.getValue());
//		Filter agg1 =searchResponse.getAggregations().get("agg1");
//		result.addAggValue("agg1", agg1.getDocCount());
//		Max max =  agg1.getAggregations().get("agg2");
//		result.addAggValue("agg2", max.getValue());
		
		return result;
	}

}
