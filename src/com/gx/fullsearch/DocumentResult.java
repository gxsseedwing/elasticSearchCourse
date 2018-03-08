package com.gx.fullsearch;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public class DocumentResult 
{	
	private long totalCount;
	
	private List<Map<String,Object>> documentList;
	
	private String scrollID;
	
	private Map<String,Object> aggValues;

	public String getScrollID() {
		return scrollID;
	}
	
	public void setScrollID(String scrollID) {
		this.scrollID = scrollID;
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public List<Map<String, Object>> getDocumentList() {
		return documentList;
	}

	public void setDocumentList(List<Map<String, Object>> documentList) {
		this.documentList = documentList;
	}
	
	public Map<String, Object> getAggValues() {
		return aggValues;
	}

	public void setAggValues(Map<String, Object> aggValues) {
		this.aggValues = aggValues;
	}

	
	public DocumentResult addAggValue(String key,Object value) 
	{
		if(null == aggValues) 
		{
			aggValues = Maps.newHashMap();
		}
		aggValues.put(key, value);
		return this;
	}
	
	@Override
	public String toString() {
		return "DocumentResult [totalCount=" + totalCount + ", documentList=" + documentList + ", scrollID=" + scrollID
				+ "]";
	}

}
