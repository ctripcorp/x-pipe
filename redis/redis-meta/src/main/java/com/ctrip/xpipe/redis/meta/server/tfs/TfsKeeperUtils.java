package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.List;

/**
 * TFS keeper classification helpers.
 */
public final class TfsKeeperUtils {

    private TfsKeeperUtils() {
    }

    public static boolean isTfsKeeper(KeeperMeta keeperMeta, DcMetaCache dcMetaCache) {
        KeeperContainerMeta keeperContainer = dcMetaCache.getKeeperContainer(keeperMeta);
        return KeeperDiskTypeUtils.isTfs(keeperContainer != null ? keeperContainer.getDiskType() : null);
    }

    public static boolean shardHasTfsKeeper(List<KeeperMeta> keepers, DcMetaCache dcMetaCache) {
        for (KeeperMeta keeperMeta : keepers) {
            if (isTfsKeeper(keeperMeta, dcMetaCache)) {
                return true;
            }
        }
        return false;
    }
}
