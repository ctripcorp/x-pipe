package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.utils.StringUtil;

public enum SentinelType {

    DR_CLUSTER,
    NON_DR_CLUSTER,
    CROSS_DC_CLUSTER;

    public static SentinelType lookupByClusterType(ClusterType clusterType) {
        if (clusterType == null) throw new IllegalArgumentException("no sentinel type for null");

        if (clusterType.equals(ClusterType.ONE_WAY) || clusterType.equals(ClusterType.BI_DIRECTION))
            return DR_CLUSTER;

        if (clusterType.equals(ClusterType.SINGLE_DC) || clusterType.equals(ClusterType.LOCAL_DC))
            return NON_DR_CLUSTER;

        if (clusterType.equals(ClusterType.CROSS_DC))
            return CROSS_DC_CLUSTER;

        throw new IllegalArgumentException("no sentinel type for cluster type " + clusterType.name());
    }

    public static SentinelType lookup(String name) {
        if (StringUtil.isEmpty(name)) throw new IllegalArgumentException("no SentinelType for name " + name);
        return valueOf(name.toUpperCase());
    }

}
