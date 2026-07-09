package com.ctrip.xpipe.redis.checker.migration.status;

/**
 * Cluster lifecycle status stored in cluster meta / DB.
 */
public enum ClusterStatus {
    Normal,
    Lock,
    Migrating,
    Rollback;

    public static boolean isSameClusterStatus(String source, ClusterStatus target) {
        return source.toLowerCase().equals(target.toString().toLowerCase());
    }

    public static boolean isMigrating(String status) {
        return status != null && isSameClusterStatus(status, Migrating);
    }

    public static ClusterStatus different(ClusterStatus clusterStatus) {
        ClusterStatus result = Normal;
        for (ClusterStatus current : values()) {
            if (!current.equals(clusterStatus)) {
                result = current;
            }
        }
        return result;
    }
}
