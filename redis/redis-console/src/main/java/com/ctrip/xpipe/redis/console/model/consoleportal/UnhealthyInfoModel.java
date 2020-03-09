package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

public class UnhealthyInfoModel {

    private int unhealthyCluster;

    private int unhealthyShard;

    private int unhealthyRedis;

    private List<String> attachFailDc;

    private Map<String, Map<String, List<HostPort>>> unhealthyInstance;

    public UnhealthyInfoModel() {
        this.attachFailDc = new ArrayList<>();
        this.unhealthyInstance = new HashMap<>();
    }

    public UnhealthyInfoModel merge(UnhealthyInfoModel other) {
        this.unhealthyCluster += other.unhealthyCluster;
        this.unhealthyShard += other.unhealthyShard;
        this.unhealthyRedis += other.unhealthyRedis;

        other.unhealthyInstance.forEach((k, v) -> {this.unhealthyInstance.put(k, v);});
        return this;
    }

    public void addUnhealthyInstance(String cluster, String dc, String shard, HostPort redis) {
        String dcShardName = dc + " " + shard;
        if (!unhealthyInstance.containsKey(cluster)) {
            unhealthyCluster++;
            this.unhealthyInstance.put(cluster, new HashMap<>());
        }

        if (!unhealthyInstance.get(cluster).containsKey(dcShardName)) {
            unhealthyShard++;
            this.unhealthyInstance.get(cluster).put(dcShardName, new ArrayList<>());
        }

        this.unhealthyRedis++;
        this.unhealthyInstance.get(cluster).get(dcShardName).add(redis);
    }

    @JsonIgnore
    public Set<String> getUnhealthyClusterNames() {
        return this.unhealthyInstance.keySet();
    }

    public String getUnhealthyClusterDesc(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return "no such cluster";
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, List<HostPort> > shard : unhealthyInstance.get(clusterName).entrySet()) {
            sb.append(shard.getKey()).append(":");
            for (HostPort redis : shard.getValue()) {
                sb.append(redis).append(",");
            }

            sb.append(";\n");
        }

        return sb.toString();
    }

    public int getUnhealthyCluster() {
        return unhealthyCluster;
    }

    public int getUnhealthyShard() {
        return unhealthyShard;
    }

    public int getUnhealthyRedis() {
        return unhealthyRedis;
    }

    public List<String> getAttachFailDc() {
        return attachFailDc;
    }

    public void setAttachFailDc(List<String> attachFailDc) {
        this.attachFailDc = attachFailDc;
    }

    public Map<String, Map<String, List<HostPort>>> getUnhealthyInstance() {
        return unhealthyInstance;
    }

    public void setUnhealthyInstance(Map<String, Map<String, List<HostPort>>> unhealthyInstance) {
        this.unhealthyInstance = unhealthyInstance;
    }
}
