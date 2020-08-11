package com.ctrip.xpipe.redis.console.model.consoleportal;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.tuple.Pair;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.*;

public class UnhealthyInfoModel {

    private int unhealthyCluster;

    private int unhealthyShard;

    private int unhealthyRedis;

    private List<String> attachFailDc;

    private Map<String, Map<String, Set<HostPort>>> unhealthyInstance;

    public UnhealthyInfoModel() {
        this.attachFailDc = new ArrayList<>();
        this.unhealthyInstance = new HashMap<>();
    }

    public UnhealthyInfoModel merge(UnhealthyInfoModel other) {
        if (null == other) return this;

        other.unhealthyInstance.forEach((cluster, shards) -> {
            shards.forEach((dcShardName, instances) -> {
                instances.forEach(redis -> {
                    this.addUnhealthyInstance(cluster, dcShardName, redis);
                });
            });
        });

        return this;
    }

    public void addUnhealthyInstance(String cluster, String dc, String shard, HostPort redis) {
        String dcShardName = dc + " " + shard;
        addUnhealthyInstance(cluster, dcShardName, redis);
    }

    private void addUnhealthyInstance(String cluster, String dcShardName, HostPort redis) {
        if (!unhealthyInstance.containsKey(cluster)) {
            unhealthyCluster++;
            this.unhealthyInstance.put(cluster, new HashMap<>());
        }

        if (!unhealthyInstance.get(cluster).containsKey(dcShardName)) {
            unhealthyShard++;
            this.unhealthyInstance.get(cluster).put(dcShardName, new HashSet<>());
        }

        if (this.unhealthyInstance.get(cluster).get(dcShardName).add(redis)) {
            unhealthyRedis++;
        }
    }

    @JsonIgnore
    public Set<String> getUnhealthyClusterNames() {
        return this.unhealthyInstance.keySet();
    }

    public List<Pair<String, String> > getUnhealthyDcShardByCluster(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return Collections.emptyList();

        List<Pair<String, String> > unhealthyDcShard = new ArrayList<>();
        for (String dcShard: this.unhealthyInstance.get(clusterName).keySet()) {
            int breakPos = dcShard.indexOf(' ');
            String dc = dcShard.substring(0, dcShard.indexOf(' '));
            String shardName = dcShard.substring(breakPos + 1, dcShard.length());
            unhealthyDcShard.add(new Pair<>(dc, shardName));
        }
        return unhealthyDcShard;
    }

    public List<String> getUnhealthyClusterDesc(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return Collections.emptyList();
        List<String> messages = new ArrayList<>();

        for (Map.Entry<String, Set<HostPort> > shard : unhealthyInstance.get(clusterName).entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(shard.getKey()).append(":");
            for (HostPort redis : shard.getValue()) {
                sb.append(redis).append(",");
            }

            sb.append(";");
            messages.add(sb.toString());
        }

        return messages;
    }

    public int countUnhealthyShardByCluster(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return 0;
        return unhealthyInstance.get(clusterName).size();
    }

    public int countUnhealthyRedisByCluster(String clusterName) {
        if (null == clusterName || !this.unhealthyInstance.containsKey(clusterName)) return 0;
        return unhealthyInstance.get(clusterName).values().stream().mapToInt(Set::size).sum();
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

    public Map<String, Map<String, Set<HostPort>>> getUnhealthyInstance() {
        return unhealthyInstance;
    }

    public void setUnhealthyInstance(Map<String, Map<String, Set<HostPort>>> unhealthyInstance) {
        this.unhealthyInstance = unhealthyInstance;
    }
}
