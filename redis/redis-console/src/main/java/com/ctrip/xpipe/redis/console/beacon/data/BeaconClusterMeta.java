package com.ctrip.xpipe.redis.console.beacon.data;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class BeaconClusterMeta {

    private Set<BeaconGroupMeta> nodeGroups;

    private Map<String, String> extra;

    public BeaconClusterMeta() {
        this.extra = Collections.emptyMap();
    }

    public BeaconClusterMeta(Set<BeaconGroupMeta> nodeGroups) {
        this();
        this.nodeGroups = nodeGroups;
    }

    public Set<BeaconGroupMeta> getNodeGroups() {
        return nodeGroups;
    }

    public void setNodeGroups(Set<BeaconGroupMeta> nodeGroups) {
        this.nodeGroups = nodeGroups;
    }

    public Map<String, String> getExtra() {
        return extra;
    }

    public void setExtra(Map<String, String> extra) {
        this.extra = extra;
    }

}
