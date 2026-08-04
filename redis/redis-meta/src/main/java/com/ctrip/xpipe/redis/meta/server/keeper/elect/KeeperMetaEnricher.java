package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Enriches ZK survive keepers with Console meta fields (e.g. priority) from {@link DcMetaCache}.
 * Priority on DcMetaCache is already normalized at load (D28); this class only copies
 * and fills default on match miss.
 */
public final class KeeperMetaEnricher {

    private static final Logger logger = LoggerFactory.getLogger(KeeperMetaEnricher.class);

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
                clone.setKeeperContainerId(fromMeta.getKeeperContainerId());
                clone.setPriority(fromMeta.getPriority());
            } else {
                clone.setPriority(KeeperPriorityUtils.DEFAULT_PRIORITY);
                logger.error("[enrich][meta match miss]cluster_{},shard_{},keeper={}, set priority={}",
                        clusterDbId, shardDbId, clone, KeeperPriorityUtils.DEFAULT_PRIORITY);
                EventMonitor.DEFAULT.logEvent(KeeperPriorityUtils.METRIC_PRIORITY_MISSING, "enrich.miss");
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
