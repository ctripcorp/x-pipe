package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;

import java.util.List;

/**
 * Strategy-aware keeper active election: priority desc with ZK order tie-break;
 * {@link KeeperElectStrategy#BM_PREFER} prefers non-TFS keepers.
 */
public class StrategyAwareKeeperActiveElectAlgorithm extends AbstractActiveElectAlgorithm {

    private final KeeperElectStrategy strategy;
    private final DcMetaCache dcMetaCache;

    public StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy strategy, DcMetaCache dcMetaCache) {
        this.strategy = strategy;
        this.dcMetaCache = dcMetaCache;
    }

    @Override
    public KeeperMeta select(Long clusterDbId, Long shardDbId, List<KeeperMeta> toBeSelected) {
        if (toBeSelected.isEmpty()) {
            return null;
        }

        List<KeeperMeta> candidates = filterElectable(toBeSelected);
        if (candidates.isEmpty()) {
            logger.warn("[select][no keeper with priority>0]cluster_{},shard_{},{}", clusterDbId, shardDbId, toBeSelected);
            return null;
        }

        if (strategy == KeeperElectStrategy.BM_PREFER) {
            List<KeeperMeta> bmCandidates = new java.util.ArrayList<>();
            for (KeeperMeta keeperMeta : candidates) {
                if (!isKeeperTfs(keeperMeta)) {
                    bmCandidates.add(keeperMeta);
                }
            }
            if (!bmCandidates.isEmpty()) {
                candidates = bmCandidates;
            }
        }

        return selectByPriorityAndZkOrder(candidates);
    }

    private List<KeeperMeta> filterElectable(List<KeeperMeta> keepers) {
        List<KeeperMeta> electable = new java.util.ArrayList<>();
        for (KeeperMeta keeperMeta : keepers) {
            if (keeperPriority(keeperMeta) > 0) {
                electable.add(keeperMeta);
            }
        }
        return electable;
    }

    private KeeperMeta selectByPriorityAndZkOrder(List<KeeperMeta> candidates) {
        KeeperMeta best = null;
        int bestPriority = -1;
        for (KeeperMeta keeperMeta : candidates) {
            int priority = keeperPriority(keeperMeta);
            if (priority > bestPriority) {
                bestPriority = priority;
                best = keeperMeta;
            }
        }
        return best;
    }

    private boolean isKeeperTfs(KeeperMeta keeperMeta) {
        KeeperContainerMeta keeperContainer = dcMetaCache.getKeeperContainer(keeperMeta);
        return KeeperDiskTypeUtils.isTfs(keeperContainer != null ? keeperContainer.getDiskType() : null);
    }

    private int keeperPriority(KeeperMeta keeperMeta) {
        Integer priority = keeperMeta.getPriority();
        return priority == null ? 0 : priority;
    }
}
