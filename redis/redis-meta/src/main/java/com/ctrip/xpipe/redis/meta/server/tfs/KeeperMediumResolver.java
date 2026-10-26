package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.utils.StringUtil;

/**
 * Decides whether a keeper container is BM (local disk) or TFS, for the release-vs-remove
 * decision — never reuse {@link TfsKeeperUtils#isTfsKeeper} here, it fails open toward BM on a
 * missing container, which is the wrong direction for a destructive action.
 * <p>
 * Requires both diskType and tfsFsId to agree on BM: a logical BU's container pool can mix
 * diskType=BM and diskType=TFS members (validateAndSetLogicalBu never reads diskType), so either
 * signal alone can misclassify. Fail-closed to TFS (false) on any ambiguity.
 */
public final class KeeperMediumResolver {

    private KeeperMediumResolver() {
    }

    public static boolean isBm(KeeperContainerMeta keeperContainer) {
        if (keeperContainer == null) {
            return false;
        }
        boolean diskTypeSaysBm = !KeeperDiskTypeUtils.isTfs(keeperContainer.getDiskType());
        boolean tfsFsIdEmpty = StringUtil.isEmpty(keeperContainer.getTfsFsId());
        return diskTypeSaysBm && tfsFsIdEmpty;
    }
}
