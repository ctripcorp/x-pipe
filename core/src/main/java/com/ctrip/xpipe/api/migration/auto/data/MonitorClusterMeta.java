package com.ctrip.xpipe.api.migration.auto.data;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.*;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class MonitorClusterMeta {

    private Set<MonitorGroupMeta> nodeGroups;

    private Map<String, String> extra;

    public MonitorClusterMeta() {
        this.extra = Collections.emptyMap();
    }

    public MonitorClusterMeta(Set<MonitorGroupMeta> nodeGroups) {
        this();
        this.nodeGroups = nodeGroups;
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

    public int generateHashCodeForBeaconCheck () {
        List<MonitorGroupMeta> nodeList = new ArrayList<>(nodeGroups);
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

}
