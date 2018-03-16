package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class MetricData {

    private String metricType;
    private String clusterName;
    private String shardName;
    private long timestampMilli;
    private long value;
    private HostPort hostPort;

    public MetricData(String metricType, String clusterName, String shardName){
        this.metricType = metricType;
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    public void setTimestampMilli(long timestampMilli) {
        this.timestampMilli = timestampMilli;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getShardName() {
        return shardName;
    }

    public long getTimestampMilli() {
        return timestampMilli;
    }

    public long getValue() {
        return value;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public String getMetricType() {
        return metricType;
    }
}

