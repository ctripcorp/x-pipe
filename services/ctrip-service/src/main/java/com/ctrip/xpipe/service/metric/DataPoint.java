package com.ctrip.xpipe.service.metric;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * May 31, 2020
 */
public class DataPoint {
    private String metric;
    private Double value;
    private Long timestamp;
    private String endpoint;
    private Map<String, String> meta = new HashMap();
    private Map<String, String> tag = new HashMap();

    public DataPoint(String metric, Double value, Long timestamp) {
        this.metric = metric;
        this.value = value;
        this.timestamp = timestamp;
    }

    public DataPoint(String metric, Double value) {
        this.metric = metric;
        this.value = value;
        this.timestamp = System.currentTimeMillis() * 1000000L;
    }

    public DataPoint() {
    }

    public String getMetric() {
        return this.metric;
    }

    public Double getValue() {
        return this.value;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public Map<String, String> getMeta() {
        return this.meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
    }

    public Map<String, String> getTag() {
        return this.tag;
    }

    public void setTag(Map<String, String> tag) {
        this.tag = tag;
    }
}
