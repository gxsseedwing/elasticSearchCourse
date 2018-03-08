package com.gx.fullsearch;

import java.util.List;
import java.util.Map;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.Lists;

public class IndexSearchHander extends IndexSearchBase
{
	String indexName = "test_index_name";
	String indexType = "test_index_type";
	
	public static void main(String[] args)
	{
		IndexSearchHander hander = new IndexSearchHander();
		//通过ID查询
//		hander.queryIndexByIdTest();
		//分页查询
//		hander.queryIndexByPageTest();
		//滚动查询
//		DocumentResult result =  hander.searchDocumentBySroclTest();
//		String scrollID =  result.getScrollID();
//		for(int i=1 ;i<(result.getTotalCount());i++) 
//		{
//			System.out.println("滚动第"+i+"次");
//			scrollID = hander.searchDocumentBySroclIDTest(scrollID);
//		}
		//聚合查询
		hander.searchDocumentByAggTest();
	}
	
	/**
	 * 通过ID查询文档
	 */
	public void queryIndexByIdTest()
	{
		String id = "1";
		Map<String, Object> map =  searchDocumentById(indexName, indexType,id);
		System.out.println(map); 
	}
	
	/**
	 * 分页查询文档
	 */
	public void queryIndexByPageTest() 
	{
		//全匹配
//		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		//词项查询
//		QueryBuilder queryBuilder = QueryBuilders.termQuery("name", "测试用户1");
		//前缀匹配
//		QueryBuilder queryBuilder = QueryBuilders.prefixQuery("name", "测试");
		//模糊匹配
//		QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name", "*3*");
		//范围查询
//		QueryBuilder queryBuilder = QueryBuilders.rangeQuery("userID").gt("2");
//		QueryBuilder queryBuilder = QueryBuilders.rangeQuery("userID").from("1", true).to("3", true);s
		//字段存在查询
//		QueryBuilder queryBuilder = QueryBuilders.existsQuery("userID");
		//嵌套查询
//		QueryBuilder queryBuilder = QueryBuilders.nestedQuery("accountList", QueryBuilders.termQuery("accountList.accountID", "3"), ScoreMode.Max);
		
		//逻辑查询
		BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
		
		//逻辑与
//		queryBuilder.must(QueryBuilders.termQuery("name", "测试用户1"));
//		queryBuilder.must(QueryBuilders.termQuery("userID", "1"));
		
		//逻辑或
//		queryBuilder.should(QueryBuilders.termQuery("name", "测试用户1"));
//		queryBuilder.should(QueryBuilders.termQuery("userID", "2"));
		
		//逻辑非
//		queryBuilder.mustNot(QueryBuilders.termQuery("name", "测试用户1"));
		
		//设置排序
		SortBuilder sortBuilder =  SortBuilders.fieldSort("userID").order(SortOrder.DESC);
		List<SortBuilder> sorts = Lists.newArrayList(sortBuilder);
		
		DocumentResult result =  searchDocumentByPage(indexName, indexType, queryBuilder, 0, 10, sorts);
		result.getDocumentList().stream().forEach(System.out::println);
	}
	
	public DocumentResult searchDocumentBySroclTest() 
	{
		System.out.println("首次滚动查询");
		SortBuilder sortBuilder =  SortBuilders.fieldSort("userID").order(SortOrder.DESC);
		List<SortBuilder> sorts = Lists.newArrayList(sortBuilder);
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		DocumentResult result = searchDocumentBySrocl(indexName, indexType, queryBuilder, 60, 1, sorts);
		result.getDocumentList().stream().forEach(System.out::println);
		return result;
	}
	
	public String searchDocumentBySroclIDTest(String scrollId) 
	{
		DocumentResult result = searchDocumentBySrocl(60, scrollId);
		result.getDocumentList().stream().forEach(System.out::println);
		return result.getScrollID();
	}
	
	public void searchDocumentByAggTest() 
	{
		QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
		
		DocumentResult result = searchDocumentByAgg(indexName, indexType, queryBuilder, 0, 10, null);
//		System.err.println(result.getAggValues());
	}
}

