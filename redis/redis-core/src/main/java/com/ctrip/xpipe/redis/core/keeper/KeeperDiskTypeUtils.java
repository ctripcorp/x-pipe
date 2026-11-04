package com.ctrip.xpipe.redis.core.keeper;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * Utility for classifying KeeperContainer disk types (BM vs TFS).
 */
public final class KeeperDiskTypeUtils {

    private KeeperDiskTypeUtils() {
    }

    public static boolean isTfs(String diskType) {
        return diskType != null && diskType.toLowerCase().startsWith("tfs");
    }

    public static boolean isBm(KeeperContainerMeta keeperContainer) {
        if (keeperContainer == null) {
            return false;
        }
        return !isTfs(keeperContainer.getDiskType());
    }
}
