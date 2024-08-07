package com.ctrip.xpipe.redis.core.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * @author lishanglin
 * date 2022/4/8
 */
public enum BeaconSystem {

    XPIPE_ONE_WAY("xpipe", Sets.immutableEnumSet(ClusterType.ONE_WAY)),
    XPIPE_BI_DIRECTION("xpipe-bi", Sets.immutableEnumSet(ClusterType.BI_DIRECTION));

    private final String systemName;

    private final Set<ClusterType> supportClusterTypes;

    BeaconSystem(String systemName, Set<ClusterType> supportClusterTypes) {
        this.systemName = systemName;
        this.supportClusterTypes = supportClusterTypes;
    }

    public String getSystemName() {
        return systemName;
    }

    public boolean support(ClusterType clusterType) {
        return supportClusterTypes.contains(clusterType);
    }

    public static BeaconSystem getDefault() {
        return XPIPE_ONE_WAY;
    }

    public static BeaconSystem findByClusterType(ClusterType clusterType) {
        for (BeaconSystem system: values()) {
            if (system.support(clusterType)) return system;
        }

        return null;
    }

    public static boolean anySupport(ClusterType clusterType) {
        return null != findByClusterType(clusterType);
    }

}
