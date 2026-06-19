package com.ctrip.xpipe.redis.core.keeper;

/**
 * Utility for classifying KeeperContainer disk types (BM vs TFS).
 */
public final class KeeperDiskTypeUtils {

    private KeeperDiskTypeUtils() {
    }

    public static boolean isTfs(String diskType) {
        return diskType != null && diskType.toLowerCase().startsWith("tfs");
    }
}
