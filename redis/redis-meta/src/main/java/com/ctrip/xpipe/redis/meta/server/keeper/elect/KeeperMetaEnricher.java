package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.ArrayList;
import java.util.List;

/**
 * Enriches ZK survive keepers with Console meta fields (e.g. priority) from {@link DcMetaCache}.
 */
public final class KeeperMetaEnricher {

    private KeeperMetaEnricher() {
    }

    public static List<KeeperMeta> enrich(DcMetaCache dcMetaCache, Long clusterDbId, Long shardDbId,
                                          List<KeeperMeta> surviveKeepers) {
        List<KeeperMeta> metaKeepers = dcMetaCache.getShardKeepers(clusterDbId, shardDbId);
        List<KeeperMeta> enriched = new ArrayList<>(surviveKeepers.size());
        for (KeeperMeta survive : surviveKeepers) {
            KeeperMeta clone = MetaCloneFacade.INSTANCE.clone(survive);
            KeeperMeta fromMeta = findMatching(metaKeepers, survive);
            if (fromMeta != null) {
                clone.setPriority(fromMeta.getPriority());
                clone.setKeeperContainerId(fromMeta.getKeeperContainerId());
            }
            enriched.add(clone);
        }
        return enriched;
    }

    private static KeeperMeta findMatching(List<KeeperMeta> metaKeepers, KeeperMeta target) {
        if (metaKeepers == null) {
            return null;
        }
        for (KeeperMeta keeperMeta : metaKeepers) {
            if (MetaUtils.same(keeperMeta, target)) {
                return keeperMeta;
            }
        }
        return null;
    }
}
