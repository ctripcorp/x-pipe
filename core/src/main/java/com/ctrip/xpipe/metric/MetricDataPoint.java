package com.ctrip.xpipe.metric;

import java.util.Map;

/**
 * @author marsqing
 *
 *         Dec 8, 2016 4:15:58 PM
 */
public class MetricDataPoint {

	private String metric;
	private long value;
	private long timestamp;
	private Map<String, String> tags;
	private Map<String, String> meta;

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public void setMeta(Map<String, String> meta) {
		this.meta = meta;
	}

	public String getMetric() {
		return metric;
	}

	public long getValue() {
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public Map<String, String> getMeta() {
		return meta;
	}

}
