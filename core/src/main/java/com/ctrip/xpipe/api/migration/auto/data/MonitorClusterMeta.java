package com.ctrip.xpipe.api.migration.auto.data;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

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

}
