package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Assigns ACTIVE / BACKUP / PREPARE per §4.4 TFS single disk slot rule.
 */
public final class KeeperRoleAssigner {

    private KeeperRoleAssigner() {
    }

    public static Map<KeeperMeta, KeeperState> assignRoles(KeeperMeta activeKeeper, List<KeeperMeta> keepers,
                                                           DcMetaCache dcMetaCache) {
        Map<KeeperMeta, KeeperState> roles = new HashMap<>();
        boolean activeIsTfs = isKeeperTfs(activeKeeper, dcMetaCache);
        KeeperMeta tfsSlotKeeper = null;
        if (!activeIsTfs) {
            tfsSlotKeeper = selectTfsSlotKeeper(activeKeeper, keepers, dcMetaCache);
        }

        for (KeeperMeta keeperMeta : keepers) {
            if (MetaUtils.same(keeperMeta, activeKeeper)) {
                roles.put(keeperMeta, KeeperState.ACTIVE);
            } else if (!isKeeperTfs(keeperMeta, dcMetaCache)) {
                roles.put(keeperMeta, KeeperState.BACKUP);
            } else if (tfsSlotKeeper != null && MetaUtils.same(keeperMeta, tfsSlotKeeper)) {
                roles.put(keeperMeta, KeeperState.BACKUP);
            } else {
                roles.put(keeperMeta, KeeperState.PREPARE);
            }
        }
        return roles;
    }

    private static KeeperMeta selectTfsSlotKeeper(KeeperMeta activeKeeper, List<KeeperMeta> keepers,
                                                  DcMetaCache dcMetaCache) {
        KeeperMeta best = null;
        int bestPriority = -1;
        for (KeeperMeta keeperMeta : keepers) {
            if (MetaUtils.same(keeperMeta, activeKeeper)) {
                continue;
            }
            if (!isKeeperTfs(keeperMeta, dcMetaCache)) {
                continue;
            }
            int priority = keeperPriority(keeperMeta);
            if (priority > bestPriority) {
                bestPriority = priority;
                best = keeperMeta;
            }
        }
        return best;
    }

    private static boolean isKeeperTfs(KeeperMeta keeperMeta, DcMetaCache dcMetaCache) {
        KeeperContainerMeta keeperContainer = dcMetaCache.getKeeperContainer(keeperMeta);
        return KeeperDiskTypeUtils.isTfs(keeperContainer != null ? keeperContainer.getDiskType() : null);
    }

    private static int keeperPriority(KeeperMeta keeperMeta) {
        Integer priority = keeperMeta.getPriority();
        return priority == null ? 0 : priority;
    }
}
