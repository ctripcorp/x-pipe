package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 17, 2017
 */
public class MetricData {

    private String metricType;
    private String dcName;
    private String clusterName;
    private String shardName;
    private String clusterType;
    private long timestampMilli;
    private double value;
    private HostPort hostPort;
    private volatile Map<String, String> tags;

    public MetricData(String metricType) {
        this(metricType, null, null, null);
    }

    public MetricData(String metricType, String dcName, String clusterName, String shardName){
        this.metricType = metricType;
        this.dcName = dcName;
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    public void setTimestampMilli(long timestampMilli) {
        this.timestampMilli = timestampMilli;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public void setClusterType(ClusterType clusterType) {
        this.clusterType = clusterType.toString();
    }

    public String getClusterType() {
        return clusterType;
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

    public double getValue() {
        return value;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public String getMetricType() {
        return metricType;
    }

    public String getDcName() {
        return dcName;
    }

    public void addTag(String key, String value) {
        if(tags == null) {
            synchronized (this) {
                if(tags == null) {
                    tags = Maps.newConcurrentMap();
                }
            }
        }
        tags.put(key, value);
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "MetricData{" +
                "metricType='" + metricType + '\'' +
                ", dcName='" + dcName + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", shardName='" + shardName + '\'' +
                ", timestampMilli=" + timestampMilli +
                ", value=" + value +
                ", hostPort=" + hostPort +
                '}';
    }
}

