package com.ctrip.xpipe.api.migration.auto.data;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class MonitorClusterMeta {

    private Set<MonitorGroupMeta> nodeGroups;

    private Set<MonitorShardMeta> shards;

    private Map<String, String> extra;

    public MonitorClusterMeta() {}

    public MonitorClusterMeta(Set<MonitorGroupMeta> nodeGroups) {
        this(nodeGroups, new HashMap<>());
    }

    public MonitorClusterMeta(Set<MonitorGroupMeta> nodeGroups, Map<String, String> extra) {
        this(nodeGroups, null, extra);
    }

    public MonitorClusterMeta(Set<MonitorGroupMeta> nodeGroups, Set<MonitorShardMeta> shards, Map<String, String> extra) {
        this.extra = extra;
        this.nodeGroups = nodeGroups;
        this.shards = shards;
    }

    public Set<MonitorGroupMeta> getNodeGroups() {
        return nodeGroups;
    }

    public void setNodeGroups(Set<MonitorGroupMeta> nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public Map<String, String> getExtra() {
        return extra;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

    public Set<MonitorShardMeta> getShards() {
        return shards;
    }

    public void setShards(Set<MonitorShardMeta> shards) {
        this.shards = shards;
    }

    public int generateHashCodeForBeaconCheck () {
        Set<MonitorGroupMeta> groupsForHash = nodeGroups;
        if ((groupsForHash == null || groupsForHash.isEmpty()) && shards != null) {
            groupsForHash = convertShardsToNodeGroups(shards);
        }
        if (groupsForHash == null) {
            groupsForHash = Collections.emptySet();
        }
        List<MonitorGroupMeta> nodeList = new ArrayList<>(groupsForHash);
        nodeList.sort(Comparator.comparing(MonitorGroupMeta::getName, Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparing(MonitorGroupMeta::getIdc, Comparator.nullsFirst(Comparator.naturalOrder()))
                .thenComparing(MonitorGroupMeta::isMasterGroup)
                .thenComparing(monitorGroupMeta -> monitorGroupMeta.getNodes().toString()));
        HashCodeBuilder builder = new HashCodeBuilder();
        for(MonitorGroupMeta group : nodeList) {
            builder.append(group.getName())
                    .append(group.getIdc())
                    .append(group.isMasterGroup())
                    .append(group.getNodes());
        }
        return builder.toHashCode();
    }

    private Set<MonitorGroupMeta> convertShardsToNodeGroups(Set<MonitorShardMeta> shardMetas) {
        Set<MonitorGroupMeta> groups = new HashSet<>();
        for (MonitorShardMeta shardMeta : shardMetas) {
            if (shardMeta == null || shardMeta.getGroups() == null) {
                continue;
            }
            for (MonitorGroupMeta groupMeta : shardMeta.getGroups()) {
                if (groupMeta == null) {
                    continue;
                }
                // For sentinel mode hash check, master flag should be ignored.
                groups.add(new MonitorGroupMeta(groupMeta.getName(), groupMeta.getIdc(), groupMeta.getNodes(), false));
            }
        }
        return groups;
    }

}
