package com.ctrip.xpipe.redis.core.meta;

/**
 * Cluster lifecycle status carried in {@link com.ctrip.xpipe.redis.core.entity.ClusterMeta}.
 */
public final class ClusterMetaStatus {

    public static final String MIGRATING = "Migrating";

    private ClusterMetaStatus() {
    }

    public static boolean isMigrating(String status) {
        return status != null && MIGRATING.equalsIgnoreCase(status);
    }
}
